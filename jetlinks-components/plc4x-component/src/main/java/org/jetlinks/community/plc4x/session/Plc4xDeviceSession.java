package org.jetlinks.community.plc4x.session;

import lombok.extern.slf4j.Slf4j;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.api.messages.PlcReadResponse;
import org.apache.plc4x.java.api.messages.PlcWriteResponse;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.jetlinks.community.plc4x.connection.Plc4xConnectionManager;
import org.jetlinks.community.plc4x.converter.Plc4xDataConverter;
import org.jetlinks.core.message.property.ReportPropertyMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
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

    // 缓存上次成功读取的值，用于降级
    private final Map<String, Object> cachedValues = new ConcurrentHashMap<>();

    // 连续失败计数器（用于监控/降级判断）
    private volatile int consecutiveFailures = 0;

    public Plc4xDeviceSession(String deviceId,
                              String connectionString,
                              long interval,
                              long timeout,
                              Plc4xConnectionManager connectionManager,
                              Map<String, String> propertyAddressMap) {
        this.deviceId = deviceId;
        this.connectionString = connectionString;
        this.interval = interval;
        this.timeout = timeout;
        this.connectionManager = connectionManager;
        this.propertyAddressMap = new ConcurrentHashMap<>(propertyAddressMap);
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
                            log.debug("Device {} adding tag: {} -> {}", deviceId, propertyId, address);
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
                        validAddressMap.forEach((propertyId, address) ->
                                builder.addTagAddress(propertyId, address, properties.get(propertyId)));

                        org.apache.plc4x.java.api.messages.PlcWriteRequest request = builder.build();
                        return Mono.fromFuture(request.execute())
                                .timeout(Duration.ofMillis(timeout))
                                .flatMap(response -> {
                                    for (String propertyId : validAddressMap.keySet()) {
                                        PlcResponseCode code = response.getResponseCode(propertyId);
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
