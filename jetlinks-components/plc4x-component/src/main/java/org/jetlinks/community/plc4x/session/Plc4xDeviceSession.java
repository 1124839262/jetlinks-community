package org.jetlinks.community.plc4x.session;

import lombok.extern.slf4j.Slf4j;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.api.messages.PlcReadResponse;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.jetlinks.community.plc4x.connection.Plc4xConnectionManager;
import org.jetlinks.community.plc4x.converter.Plc4xDataConverter;
import org.jetlinks.core.message.property.ReportPropertyMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PLC4X Device Session for managing device communication
 */
@Slf4j
public class Plc4xDeviceSession {

    private final String deviceId;
    private final String connectionString;
    private final long interval;
    private final long timeout;
    private final Plc4xConnectionManager connectionManager;
    private final Map<String, String> propertyAddressMap;
    private final Map<String, String> propertyTypeMap;

    // 缓存上次成功读取的值，用于降级
    private final Map<String, Object> cachedValues = new ConcurrentHashMap<>();

    // 连续失败计数器（用于监控/降级判断）
    private volatile int consecutiveFailures = 0;

    public Plc4xDeviceSession(String deviceId,
                              String connectionString,
                              long interval,
                              long timeout,
                              Plc4xConnectionManager connectionManager,
                              Map<String, String> propertyAddressMap,
                              Map<String, String> propertyTypeMap) {
        this.deviceId = deviceId;
        this.connectionString = connectionString;
        this.interval = interval;
        this.timeout = timeout;
        this.connectionManager = connectionManager;
        this.propertyAddressMap = new ConcurrentHashMap<>(propertyAddressMap);
        this.propertyTypeMap = new ConcurrentHashMap<>(propertyTypeMap);
    }

    /**
     * Read all properties from PLC device with fallback to cached values
     */
    public Mono<Map<String, Object>> readAllProperties() {
        Map<String, String> validAddressMap = validAddressMap();
        if (validAddressMap.isEmpty()) {
            log.error("Device {} has no property with valid PLC address configured, nothing to read. " +
                    "请在物模型属性的 'PLC地址' 字段配置节点地址, 例如: ns=2;s=模拟器示例.函数.Ramp1", deviceId);
            return Mono.empty();
        }

        return readProperties(validAddressMap, true);
    }

    /**
     * Read all properties without cache fallback. Used by command reads that must return an explicit reply.
     */
    public Mono<Map<String, Object>> readAllPropertiesStrict() {
        Map<String, String> validAddressMap = validAddressMap();
        if (validAddressMap.isEmpty()) {
            return Mono.error(new IllegalArgumentException(
                    "Device " + deviceId + " has no property with valid PLC address configured"));
        }

        return readProperties(validAddressMap, false);
    }

    /**
     * Read selected properties from PLC device.
     */
    public Mono<Map<String, Object>> readProperties(Collection<String> propertyIds) {
        if (propertyIds == null || propertyIds.isEmpty()) {
            return readAllProperties();
        }

        Map<String, String> selectedAddressMap = new LinkedHashMap<>();
        for (String propertyId : propertyIds) {
            String address = propertyAddressMap.get(propertyId);
            if (address == null || address.trim().isEmpty()) {
                return Mono.error(new IllegalArgumentException(
                        "Property " + propertyId + " has no PLC address configured"));
            }
            selectedAddressMap.put(propertyId, address.trim());
        }

        return readProperties(selectedAddressMap, true);
    }

    /**
     * Read selected properties without cache fallback.
     */
    public Mono<Map<String, Object>> readPropertiesStrict(Collection<String> propertyIds) {
        if (propertyIds == null || propertyIds.isEmpty()) {
            return readAllPropertiesStrict();
        }

        Map<String, String> selectedAddressMap = new LinkedHashMap<>();
        for (String propertyId : propertyIds) {
            String address = propertyAddressMap.get(propertyId);
            if (address == null || address.trim().isEmpty()) {
                return Mono.error(new IllegalArgumentException(
                        "Property " + propertyId + " has no PLC address configured"));
            }
            selectedAddressMap.put(propertyId, address.trim());
        }

        return readProperties(selectedAddressMap, false);
    }

    private Mono<Map<String, Object>> readProperties(Map<String, String> validAddressMap, boolean fallbackToCache) {
        return connectionManager.getConnection(deviceId, connectionString)
                .flatMap(connection -> {
                    try {
                        org.apache.plc4x.java.api.messages.PlcReadRequest.Builder builder = connection.readRequestBuilder();
                        validAddressMap.forEach((propertyId, address) -> {
                            //log.debug("Device {} adding tag: {} -> {}", deviceId, propertyId, address);
                            builder.addTagAddress(propertyId, address);
                        });

                        org.apache.plc4x.java.api.messages.PlcReadRequest request = builder.build();
                        return Mono.fromFuture(request.execute())
                                .timeout(Duration.ofMillis(timeout))
                                .map(response -> parseReadResponse(response, validAddressMap.keySet(), !fallbackToCache))
                                .flatMap(result -> {
                                    if (!fallbackToCache && result.isEmpty()) {
                                        return Mono.error(new IllegalStateException(
                                                "PLC read returned no data for device " + deviceId));
                                    }
                                    return Mono.just(result);
                                })
                                .doOnSuccess(result -> {
                                    // 读取成功，重置失败计数并更新缓存
                                    consecutiveFailures = 0;
                                    updateCache(result);
                                });
                    } catch (Exception e) {
                        return Mono.error(new PlcConnectionException("Failed to build read request", e));
                    }
                })
                .onErrorResume(e -> handleReadFailure(e, validAddressMap.keySet(), fallbackToCache));
    }

    /**
     * Read a single property with fallback to cached value
     */
    public Mono<Object> readProperty(String propertyId) {
        return readProperties(Collections.singletonList(propertyId))
                .flatMap(values -> Mono.justOrEmpty(values.get(propertyId)));
    }

    /**
     * Write a property value to PLC
     */
    public Mono<Void> writeProperty(String propertyId, Object value) {
        return writeProperties(Collections.singletonMap(propertyId, value));
    }

    /**
     * Write property values to PLC.
     */
    public Mono<Void> writeProperties(Map<String, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return Mono.empty();
        }

        Map<String, String> validAddressMap = new LinkedHashMap<>();
        for (String propertyId : properties.keySet()) {
            String address = propertyAddressMap.get(propertyId);
            if (address == null || address.trim().isEmpty()) {
                return Mono.error(new IllegalArgumentException(
                        "Property " + propertyId + " has no PLC address configured"));
            }
            validAddressMap.put(propertyId, address.trim());
        }

        return connectionManager.getConnection(deviceId, connectionString)
                .flatMap(connection -> {
                    try {
                        org.apache.plc4x.java.api.messages.PlcWriteRequest.Builder builder = connection.writeRequestBuilder();
                        validAddressMap.forEach((propertyId, address) -> {
                            Object rawValue = properties.get(propertyId);
                            Object value = convertWriteValue(propertyId, rawValue);
                            log.debug("Writing PLC property, deviceId={}, propertyId={}, address={}, metadataType={}, value={}, valueType={}, rawValue={}, rawValueType={}",
                                    deviceId, propertyId, address, propertyTypeMap.get(propertyId), value,
                                    value == null ? null : value.getClass().getName(), rawValue,
                                    rawValue == null ? null : rawValue.getClass().getName());
                            builder.addTagAddress(propertyId, address, value);
                        });

                        org.apache.plc4x.java.api.messages.PlcWriteRequest request = builder.build();
                        return Mono.fromFuture(request.execute())
                                .timeout(Duration.ofMillis(timeout))
                                .flatMap(response -> {
                                    for (String propertyId : validAddressMap.keySet()) {
                                        PlcResponseCode code = response.getResponseCode(propertyId);
                                        log.debug("PLC property write response, deviceId={}, propertyId={}, address={}, code={}",
                                                deviceId, propertyId, validAddressMap.get(propertyId), code);
                                        if (code != PlcResponseCode.OK) {
                                            return Mono.error(new RuntimeException(
                                                    "Failed to write property: " + propertyId + ", code: " + code));
                                        }
                                    }
                                    updateCache(properties);
                                    return Mono.empty();
                                });
                    } catch (Exception e) {
                        return Mono.error(new PlcConnectionException("Failed to write properties", e));
                    }
                });
    }

    private Object convertWriteValue(String propertyId, Object value) {
        if (value == null) {
            return null;
        }
        String type = propertyTypeMap.get(propertyId);
        if (type == null || type.trim().isEmpty()) {
            return value;
        }

        String normalizedType = type.toLowerCase(Locale.ROOT).trim();

        // 处理数组类型
        if (normalizedType.endsWith("[]") || normalizedType.contains("array")) {
            return convertArrayValue(normalizedType, value);
        }

        try {
            switch (normalizedType) {
                // Byte types
                case "byte":
                case "uint8":
                case "usint":
                    if (value instanceof Byte) return value;
                    return value instanceof Number
                        ? ((Number) value).byteValue()
                        : Byte.parseByte(value.toString().trim());

                // Short types (int16)
                case "short":
                case "int16":
                case "sint":
                    if (value instanceof Short) return value;
                    return value instanceof Number
                        ? ((Number) value).shortValue()
                        : Short.parseShort(value.toString().trim());

                // Unsigned short (uint16/word) -> int
                case "uint16":
                case "word":
                case "ushort":
                    if (value instanceof Integer) return value;
                    int uint16Val = value instanceof Number
                        ? ((Number) value).intValue()
                        : Integer.parseInt(value.toString().trim());
                    return uint16Val & 0xFFFF; // 确保无符号范围 [0, 65535]

                // Integer types (int32)
                case "int":
                case "integer":
                case "int32":
                case "dint":
                    if (value instanceof Integer) return value;
                    return value instanceof Number
                        ? ((Number) value).intValue()
                        : Integer.parseInt(value.toString().trim());

                // Unsigned int (uint32/dword) -> long
                case "uint32":
                case "dword":
                case "uint":
                case "udint":
                    if (value instanceof Long) return value;
                    long uint32Val = value instanceof Number
                        ? ((Number) value).longValue()
                        : Long.parseLong(value.toString().trim());
                    return uint32Val & 0xFFFFFFFFL; // 确保无符号范围 [0, 4294967295]

                // Long types (int64)
                case "long":
                case "int64":
                case "lint":
                    if (value instanceof Long) return value;
                    return value instanceof Number
                        ? ((Number) value).longValue()
                        : Long.parseLong(value.toString().trim());

                // Unsigned long (uint64) -> BigInteger (实际场景中较少使用)
                case "uint64":
                case "lword":
                case "ulint":
                    if (value instanceof Long) return value;
                    // PLC4X 通常使用 long，无法完全表示 uint64
                    return value instanceof Number
                        ? ((Number) value).longValue()
                        : Long.parseUnsignedLong(value.toString().trim());

                // Float types
                case "float":
                case "real":
                    if (value instanceof Float) return value;
                    return value instanceof Number
                        ? ((Number) value).floatValue()
                        : Float.parseFloat(value.toString().trim());

                // Double types
                case "double":
                case "lreal":
                    if (value instanceof Double) return value;
                    return value instanceof Number
                        ? ((Number) value).doubleValue()
                        : Double.parseDouble(value.toString().trim());

                // Boolean types (支持多种格式)
                case "boolean":
                case "bool":
                    if (value instanceof Boolean) return value;
                    return parseBooleanValue(value.toString().trim());

                // String types
                case "string":
                case "str":
                    return value.toString();

                default:
                    return value;
            }
        } catch (Exception e) {
            log.warn("Failed to convert PLC write value, deviceId=, propertyId={}, metadataType={}, value={}, valueType={}",
                    deviceId, propertyId, type, value, value.getClass().getName(), e);
            return value;
        }
    }

    /**
     * Convert array values for PLC write operations.
     * Supports: int[], float[], double[], boolean[], string[], byte[], short[], long[]
     */
    private Object convertArrayValue(String arrayType, Object value) {
        // 已经是数组类型，直接返回
        if (value.getClass().isArray()) {
            return value;
        }

        // 集合类型转数组
        if (value instanceof java.util.Collection) {
            java.util.Collection<?> collection = (java.util.Collection<?>) value;
            String elementType = extractElementType(arrayType);
            return convertCollectionToArray(elementType, collection);
        }

        // 字符串格式 "[1,2,3]" 或 "1,2,3"
        if (value instanceof String) {
            String str = value.toString().trim();
            if (str.startsWith("[") && str.endsWith("]")) {
                str = str.substring(1, str.length() - 1);
            }
            String[] parts = str.split(",");
            String elementType = extractElementType(arrayType);
            return convertStringArrayToTypedArray(elementType, parts);
        }

        return value;
    }

    private String extractElementType(String arrayType) {
        // "int[]" -> "int"
        // "integer array" -> "integer"
        if (arrayType.endsWith("[]")) {
            return arrayType.substring(0, arrayType.length() - 2).trim();
        }
        if (arrayType.contains("array")) {
            return arrayType.replace("array", "").trim();
        }
        return "string";
    }

    private Object convertCollectionToArray(String elementType, java.util.Collection<?> collection) {
        String normalized = elementType.toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "int":
            case "integer":
            case "int32":
            case "dint":
                return collection.stream()
                    .map(v -> v instanceof Number ? ((Number) v).intValue() : Integer.parseInt(v.toString().trim()))
                    .toArray(Integer[]::new);

            case "float":
            case "real":
                return collection.stream()
                    .map(v -> v instanceof Number ? ((Number) v).floatValue() : Float.parseFloat(v.toString().trim()))
                    .toArray(Float[]::new);

            case "double":
            case "lreal":
                return collection.stream()
                    .map(v -> v instanceof Number ? ((Number) v).doubleValue() : Double.parseDouble(v.toString().trim()))
                    .toArray(Double[]::new);

            case "boolean":
            case "bool":
                return collection.stream()
                    .map(v -> v instanceof Boolean ? (Boolean) v : parseBooleanValue(v.toString().trim()))
                    .toArray(Boolean[]::new);

            case "byte":
            case "uint8":
            case "usint":
                return collection.stream()
                    .map(v -> v instanceof Number ? ((Number) v).byteValue() : Byte.parseByte(v.toString().trim()))
                    .toArray(Byte[]::new);

            case "short":
            case "int16":
            case "sint":
                return collection.stream()
                    .map(v -> v instanceof Number ? ((Number) v).shortValue() : Short.parseShort(v.toString().trim()))
                    .toArray(Short[]::new);

            case "long":
            case "int64":
            case "lint":
                return collection.stream()
                    .map(v -> v instanceof Number ? ((Number) v).longValue() : Long.parseLong(v.toString().trim()))
                    .toArray(Long[]::new);

            default:
                return collection.stream()
                    .map(Object::toString)
                    .toArray(String[]::new);
        }
    }

    private Object convertStringArrayToTypedArray(String elementType, String[] parts) {
        String normalized = elementType.toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "int":
            case "integer":
            case "int32":
            case "dint":
                return java.util.Arrays.stream(parts)
                    .map(String::trim)
                    .map(Integer::parseInt)
                    .toArray(Integer[]::new);

            case "float":
            case "real":
                return java.util.Arrays.stream(parts)
                    .map(String::trim)
                    .map(Float::parseFloat)
                    .toArray(Float[]::new);

            case "double":
            case "lreal":
                return java.util.Arrays.stream(parts)
                    .map(String::trim)
                    .map(Double::parseDouble)
                    .toArray(Double[]::new);

            case "boolean":
            case "bool":
                return java.util.Arrays.stream(parts)
                    .map(String::trim)
                    .map(this::parseBooleanValue)
                    .toArray(Boolean[]::new);

            case "byte":
            case "uint8":
            case "usint":
                return java.util.Arrays.stream(parts)
                    .map(String::trim)
                    .map(Byte::parseByte)
                    .toArray(Byte[]::new);

            case "short":
            case "int16":
            case "sint":
                return java.util.Arrays.stream(parts)
                    .map(String::trim)
                    .map(Short::parseShort)
                    .toArray(Short[]::new);

            case "long":
            case "int64":
            case "lint":
                return java.util.Arrays.stream(parts)
                    .map(String::trim)
                    .map(Long::parseLong)
                    .toArray(Long[]::new);

            default:
                return java.util.Arrays.stream(parts)
                    .map(String::trim)
                    .toArray(String[]::new);
        }
    }

    /**
     * Parse boolean value from string, supporting multiple formats:
     * - "true"/"false" (case-insensitive)
     * - "1"/"0"
     * - "yes"/"no"
     * - "on"/"off"
     */
    private boolean parseBooleanValue(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        String normalized = str.toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "true":
            case "1":
            case "yes":
            case "on":
                return true;
            case "false":
            case "0":
            case "no":
            case "off":
                return false;
            default:
                return Boolean.parseBoolean(normalized);
        }
    }

    /**
     * Start polling properties at configured interval
     */
    public Flux<ReportPropertyMessage> startPolling() {
        return Flux.interval(Duration.ofMillis(interval))
                .onBackpressureDrop()
                .flatMap(tick -> readAllProperties()
                        // 没有任何有效属性值时不上报空消息
                        .filter(properties -> !properties.isEmpty())
                        .map(properties -> {
                            ReportPropertyMessage message = new ReportPropertyMessage();
                            message.setDeviceId(deviceId);
                            message.setTimestamp(System.currentTimeMillis());
                            message.setProperties(properties);
                            return message;
                        })
                        .onErrorResume(e -> {
                            // 单次读取失败不终止轮询，连接层 health-check 会自动重连
                            log.warn("Error polling properties for device {}: {}", deviceId, e.getMessage());
                            return Mono.empty();
                        }), 1);
    }

    /**
     * Close the device session
     */
    public Mono<Void> close() {
        return connectionManager.closeConnection(deviceId)
                .doOnSuccess(v -> {
                    cachedValues.clear();
                    log.info("Closed device session: {}", deviceId);
                });
    }

    /**
     * Parse PLC read response to property map
     */
    private Map<String, Object> parseReadResponse(PlcReadResponse response,
                                                  Collection<String> expectedTagNames,
                                                  boolean failOnError) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (String tagName : expectedTagNames) {
            PlcResponseCode code = response.getResponseCode(tagName);
            if (code == PlcResponseCode.OK) {
                result.put(tagName, Plc4xDataConverter.toJavaObject(response.getPlcValue(tagName)));
            } else {
                if (failOnError) {
                    throw new IllegalStateException("Failed to read property " + tagName + ", code: " + code);
                }
                log.warn("Failed to read property {}: {}", tagName, code);
            }
        }
        return result;
    }

    private Map<String, String> validAddressMap() {
        Map<String, String> validAddressMap = new LinkedHashMap<>();
        propertyAddressMap.forEach((propertyId, address) -> {
            if (address != null && !address.trim().isEmpty()) {
                validAddressMap.put(propertyId, address.trim());
            } else {
                log.warn("Device {} property '{}' has no PLC address configured (expands.address), skipping",
                        deviceId, propertyId);
            }
        });
        return validAddressMap;
    }

    private void updateCache(Map<String, Object> values) {
        values.forEach((property, value) -> {
            if (value == null) {
                cachedValues.remove(property);
            } else {
                cachedValues.put(property, value);
            }
        });
    }

    private Mono<Map<String, Object>> handleReadFailure(Throwable e,
                                                       Collection<String> propertyIds,
                                                       boolean fallbackToCache) {
        consecutiveFailures++;
        log.warn("Failed to read properties from device {} (consecutive failures: {}): {}",
                deviceId, consecutiveFailures, e.getMessage());

        if (!fallbackToCache) {
            return Mono.error(e);
        }

        if (!cachedValues.isEmpty()) {
            log.debug("Using cached values for device {}", deviceId);
            return cachedValues(propertyIds);
        }

        // 无缓存则跳过本次上报，下一轮 Flux.interval 会继续重试。
        return Mono.empty();
    }

    private Mono<Map<String, Object>> cachedValues(Collection<String> propertyIds) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (String propertyId : propertyIds) {
            if (cachedValues.containsKey(propertyId)) {
                values.put(propertyId, cachedValues.get(propertyId));
            }
        }
        return values.isEmpty() ? Mono.empty() : Mono.just(values);
    }

    public String getDeviceId() {
        return deviceId;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public long getInterval() {
        return interval;
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures;
    }
}
