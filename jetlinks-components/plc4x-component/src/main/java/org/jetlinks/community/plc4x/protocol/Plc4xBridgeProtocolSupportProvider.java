/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.community.plc4x.protocol;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.gateway.DeviceGatewayHelper;
import org.jetlinks.community.plc4x.Plc4xProperties;
import org.jetlinks.community.plc4x.connection.Plc4xConnectionManager;
import org.jetlinks.community.plc4x.session.Plc4xDeviceSession;
import org.jetlinks.community.plc4x.session.Plc4xPlatformDeviceSession;
import org.jetlinks.community.plc4x.util.Plc4xConnectionStringUtils;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.device.*;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.*;
import org.jetlinks.community.ConfigMetadataConstants;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.core.message.property.ReadPropertyMessageReply;
import org.jetlinks.core.message.property.WritePropertyMessage;
import org.jetlinks.core.message.property.WritePropertyMessageReply;
import org.jetlinks.core.metadata.*;
import org.jetlinks.core.metadata.types.BooleanType;
import org.jetlinks.core.metadata.types.LongType;
import org.jetlinks.core.metadata.types.PasswordType;
import org.jetlinks.core.metadata.types.StringType;
import org.jetlinks.core.spi.ProtocolSupportProvider;
import org.jetlinks.core.spi.ServiceContext;
import org.jetlinks.supports.official.JetLinksDeviceMetadataCodec;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PLC4X Bridge Protocol Support Provider
 */
@Slf4j
public class Plc4xBridgeProtocolSupportProvider implements ProtocolSupportProvider {

    // 平台消息处理依赖，由 Spring 在 Plc4xAutoConfiguration 中注入；
    // 若为 null（例如通过上传 jar 包方式加载协议），则降级为只采集不上报。
    private final DeviceGatewayHelper gatewayHelper;

    private final Plc4xProperties properties;

    public Plc4xBridgeProtocolSupportProvider() {
        this(null, null);
    }

    public Plc4xBridgeProtocolSupportProvider(DeviceGatewayHelper gatewayHelper) {
        this(gatewayHelper, null);
    }

    public Plc4xBridgeProtocolSupportProvider(DeviceGatewayHelper gatewayHelper, Plc4xProperties properties) {
        this.gatewayHelper = gatewayHelper;
        this.properties = properties;
    }

    @Override
    public Mono<ProtocolSupport> create(ServiceContext context) {
        // 从配置中获取参数
        Plc4xProperties supportProperties = Optional.ofNullable(context)
            .flatMap(ctx -> ctx.getService(Plc4xProperties.class))
            .orElseGet(() -> properties == null ? new Plc4xProperties() : properties);

        Plc4xConnectionManager connectionManager = new Plc4xConnectionManager(
            supportProperties.getMaxPoolSize(),
            supportProperties.isHealthCheckEnabled(),
            supportProperties.getHealthCheckInterval()
        );

        Map<String, Plc4xDeviceSession> sessionMap = new ConcurrentHashMap<>();
        Map<String, Disposable> pollingSubscriptionMap = new ConcurrentHashMap<>();
        Map<String, Object> sessionLockMap = new ConcurrentHashMap<>();

        return Mono.just(new Plc4xBridgeProtocolSupport(
            connectionManager,
            sessionMap,
            pollingSubscriptionMap,
            sessionLockMap,
            gatewayHelper,
            supportProperties));
    }

    /**
     * PLC4X Bridge Protocol Support Implementation
     */
    @Slf4j
    static class Plc4xBridgeProtocolSupport implements ProtocolSupport {

        private static final String ID = "plc4x-bridge";
        private static final String NAME = "PLC4X Bridge Protocol";
        private static final String DESCRIPTION = "Unified PLC protocol bridge based on Apache PLC4X, supporting S7, Modbus, OPC UA, EtherNet/IP, ADS, and more";
        private static final JetLinksDeviceMetadataCodec metadataCodec = new JetLinksDeviceMetadataCodec();
        private static final Duration SESSION_CLOSE_TIMEOUT = Duration.ofSeconds(10);

        private final Plc4xConnectionManager connectionManager;
        private final Map<String, Plc4xDeviceSession> sessionMap;
        private final Map<String, Disposable> pollingSubscriptionMap;
        private final Map<String, Object> sessionLockMap;
        private final DeviceGatewayHelper gatewayHelper;
        private final Plc4xProperties properties;

        public Plc4xBridgeProtocolSupport(Plc4xConnectionManager connectionManager,
                                          Map<String, Plc4xDeviceSession> sessionMap,
                                          Map<String, Disposable> pollingSubscriptionMap,
                                          Map<String, Object> sessionLockMap,
                                          DeviceGatewayHelper gatewayHelper,
                                          Plc4xProperties properties) {
            this.connectionManager = connectionManager;
            this.sessionMap = sessionMap;
            this.pollingSubscriptionMap = pollingSubscriptionMap;
            this.sessionLockMap = sessionLockMap;
            this.gatewayHelper = gatewayHelper;
            this.properties = properties;
        }

        @Override
        public String getId() {
            return ID;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public String getDescription() {
            return DESCRIPTION;
        }

        @Override
        public Flux<? extends Transport> getSupportedTransport() {
            return Flux.just(DefaultTransport.TCP);
        }

        @Nonnull
        @Override
        public Mono<? extends DeviceMessageCodec> getMessageCodec(Transport transport) {
            return Mono.just(new Plc4xMessageCodec(sessionMap));
        }

        @Nonnull
        @Override
        public DeviceMetadataCodec getMetadataCodec() {
            return metadataCodec;
        }

        @Override
        public Mono<ConfigMetadata> getConfigMetadata(Transport transport) {
            return Mono.just(new DefaultConfigMetadata(
                    "PLC4X Connection Configuration",
                    "配置 PLC4X 连接参数，支持多种工业协议"
            ).add("connectionString", "连接字符串",
                    "PLC4X 连接字符串 | 格式: {protocol}:{transport}://{host}:{port}?{parameters}\n\n" +
                    "连接字符串示例:\n" +
                    "  • S7: s7://192.168.1.10?remote-rack=0&remote-slot=1\n" +
                    "  • Modbus: modbus-tcp://192.168.1.20:502\n" +
                    "  • OPC UA (匿名): opcua:tcp://127.0.0.1:49320\n" +
                    "  • OPC UA (认证): opcua:tcp://127.0.0.1:49320 (用户名密码使用下方字段配置)\n\n" +
                    "物模型属性地址格式 (expands.address 字段):\n" +
                    "  • S7: %DB1.DBW0:INT (数据块1, 字偏移0, INT类型)\n" +
                    "  • Modbus: holding-register:1:INT (保持寄存器, 地址1, INT类型)\n" +
                    "  • OPC UA: ns=2;s=Temperature (命名空间2, 字符串标识符)\n" +
                    "  • OPC UA: ns=1;i=1001 (命名空间1, 数字标识符)\n\n" +
                    "文档: https://plc4x.apache.org/plc4x/0.13.0/users/protocols/",
                    new StringType().expand(ConfigMetadataConstants.maxLength, 500L))
                    .add("username", "用户名",
                        "认证用户名（可选）\n用于 OPC UA、S7 等需要认证的协议\n优先级高于连接字符串中的 username 参数",
                        new StringType().expand(ConfigMetadataConstants.maxLength, 100L))
                    .add("password", "密码",
                        "认证密码（可选）\n配合用户名使用\n优先级高于连接字符串中的 password 参数",
                        new PasswordType().expand(ConfigMetadataConstants.maxLength, 100L))
                    .add("interval", "采集间隔(ms)",
                        "数据采集频率，单位毫秒\n默认: 5000ms\n建议: 1000-60000ms",
                        new LongType())
                    .add("timeout", "超时时间(ms)",
                        "请求超时时间，单位毫秒\n默认: 10000ms\n建议: 1000-60000ms",
                        new LongType())
                    .add("autoSubscribe", "自动订阅",
                        "启用后自动订阅物模型属性并定时采集数据\n禁用后需手动调用设备功能进行读写",
                        new BooleanType()));
        }

        @Nonnull
        @Override
        public Mono<AuthenticationResponse> authenticate(@Nonnull AuthenticationRequest request, @Nonnull DeviceOperator deviceOperation) {
            return Mono.just(AuthenticationResponse.success(deviceOperation.getDeviceId()));
        }

        @Nonnull
        @Override
        public Mono<AuthenticationResponse> authenticate(@Nonnull AuthenticationRequest request, @Nonnull DeviceRegistry registry) {
            return Mono.just(AuthenticationResponse.success());
        }

        @Override
        public Mono<Void> onDeviceRegister(DeviceOperator operator) {
            return operator.getMetadata()
                    .flatMap(metadata -> Mono.zip(
                            Flux.fromIterable(metadata.getProperties())
                                    .collectMap(
                                            prop -> prop.getId(),
                                            prop -> prop.getExpand("address")
                                                    .map(Object::toString)
                                                    .orElse("")
                                    ),
                            Flux.fromIterable(metadata.getProperties())
                                    .collectMap(
                                            prop -> prop.getId(),
                                            prop -> Optional.ofNullable(prop.getValueType())
                                                    .map(DataType::getType)
                                                    .orElse("")
                                    )
                    ))
                    .flatMap(tuple -> {
                        Map<String, String> addressMap = tuple.getT1();
                        Map<String, String> typeMap = tuple.getT2();
                        return operator.getConfig("connectionString")
                                .map(Object::toString)
                                .switchIfEmpty(Mono.defer(() -> {
                                    log.error("Device {} has no connectionString configured in device config", operator.getDeviceId());
                                    return stopDeviceSession(operator.getDeviceId()).then(Mono.<String>empty());
                                }))
                                .flatMap(connectionString -> createDeviceSession(operator, connectionString, addressMap, typeMap));
                    })
                    .onErrorResume(e -> {
                        log.error("Failed to create PLC session for device {}: {}", operator.getDeviceId(), e.getMessage(), e);
                        return Mono.empty();
                    });
        }

        private Mono<Void> createDeviceSession(DeviceOperator operator,
                                               String connectionString,
                                               Map<String, String> addressMap,
                                               Map<String, String> typeMap) {
            String deviceId = operator.getDeviceId();
            if (connectionString == null || connectionString.trim().isEmpty()) {
                log.warn("Device {} has no connection string configured", operator.getDeviceId());
                return stopDeviceSession(deviceId);
            }

            Mono<Long> intervalMono = operator.getConfig("interval")
                    .map(v -> parsePositiveLong(v.toString(), properties.getDefaultInterval()))
                    .defaultIfEmpty(properties.getDefaultInterval());

            Mono<Long> timeoutMono = operator.getConfig("timeout")
                    .map(v -> parsePositiveLong(v.toString(), properties.getDefaultTimeout()))
                    .defaultIfEmpty(properties.getDefaultTimeout());

            Mono<Boolean> autoSubscribeMono = operator.getConfig("autoSubscribe")
                    .map(v -> Boolean.parseBoolean(v.toString()))
                    .defaultIfEmpty(properties.isAutoSubscribe());

            // 获取分离的认证字段
            Mono<String> usernameMono = operator.getConfig("username")
                    .map(Object::toString)
                    .defaultIfEmpty("");

            Mono<String> passwordMono = operator.getConfig("password")
                    .map(Object::toString)
                    .defaultIfEmpty("");

            return Mono.zip(intervalMono, timeoutMono, autoSubscribeMono, usernameMono, passwordMono)
                    .flatMap(tuple -> {
                        long interval = tuple.getT1();
                        long timeout = tuple.getT2();
                        boolean autoSubscribe = tuple.getT3();
                        String username = tuple.getT4();
                        String password = tuple.getT5();

                        String finalConnectionString = Plc4xConnectionStringUtils.withAuthentication(
                                connectionString.trim(), username, password);
                        return replaceDeviceSession(operator, deviceId, finalConnectionString,
                                interval, timeout, autoSubscribe, addressMap, typeMap);
                    });
        }

        private Mono<Void> replaceDeviceSession(DeviceOperator operator,
                                                String deviceId,
                                                String connectionString,
                                                long interval,
                                                long timeout,
                                                boolean autoSubscribe,
                                                Map<String, String> addressMap,
                                                Map<String, String> typeMap) {
            return Mono.fromRunnable(() -> {
                        Object lock = sessionLockMap.computeIfAbsent(deviceId, key -> new Object());
                        synchronized (lock) {
                            disposePolling(deviceId);
                            closeSession(deviceId, sessionMap.remove(deviceId));

                            Plc4xDeviceSession session = new Plc4xDeviceSession(
                                    deviceId,
                                    connectionString,
                                    interval,
                                    timeout,
                                    connectionManager,
                                    addressMap,
                                    typeMap
                            );
                            sessionMap.put(deviceId, session);

                            if (autoSubscribe) {
                                Disposable disposable = session.startPolling()
                                        .flatMap(message -> reportToPlatform(operator, message))
                                        .onErrorContinue((e, o) -> log.error(
                                                "Error reporting property for device {}: {}", deviceId, e.getMessage(), e))
                                        .subscribe();
                                Disposable previous = pollingSubscriptionMap.put(deviceId, disposable);
                                if (previous != null) {
                                    previous.dispose();
                                }
                            }

                            log.info("Device {} registered, PLC session created", deviceId);
                        }
                    })
                    .subscribeOn(Schedulers.boundedElastic())
                    .then();
        }

        /**
         * 将轮询采集到的属性上报到 JetLinks 平台。
         *
         * <p>通过 {@link DeviceGatewayHelper#handleDeviceMessage} 统一处理：
         * 创建/维持设备会话使设备上线，并将消息交给平台进行属性存储与规则触发。
         */
        private Mono<Void> reportToPlatform(DeviceOperator operator,
                                            org.jetlinks.core.message.property.ReportPropertyMessage message) {
            if (gatewayHelper == null) {
                // 未注入平台依赖时降级为仅日志（例如非 Spring Bean 方式加载协议）
                log.debug("Property report from device {} (no gateway, not reported): {}",
                        operator.getDeviceId(), message.getProperties());
                return Mono.empty();
            }
            return gatewayHelper
                    .handleDeviceMessage(
                            message,
                            device -> new Plc4xPlatformDeviceSession(device),
                            session -> {
                                // 每次上报刷新保活
                                session.keepAlive();
                            },
                            () -> log.warn("Cannot report PLC data: device {} not found in registry", operator.getDeviceId())
                    )
                    .then();
        }

        private long parsePositiveLong(String value, long defaultValue) {
            try {
                long parsed = Long.parseLong(value);
                return parsed > 0 ? parsed : defaultValue;
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }

        @Override
        public Mono<Void> onDeviceUnRegister(DeviceOperator operator) {
            return stopDeviceSession(operator.getDeviceId())
                    .doOnSuccess(v -> log.info("Device {} unregistered, PLC session closed", operator.getDeviceId()));
        }

        @Override
        public Mono<Void> onDeviceMetadataChanged(DeviceOperator operator) {
            return onDeviceRegister(operator);
        }

        @Override
        public void dispose() {
            pollingSubscriptionMap.values().forEach(Disposable::dispose);
            pollingSubscriptionMap.clear();
            sessionMap.clear();
            sessionLockMap.clear();
            connectionManager.closeAll()
                    .doOnError(e -> log.error("Error closing PLC4X connection manager: {}", e.getMessage(), e))
                    .subscribe();
        }

        private Mono<Void> stopDeviceSession(String deviceId) {
            return Mono.fromRunnable(() -> {
                        Object lock = sessionLockMap.computeIfAbsent(deviceId, key -> new Object());
                        synchronized (lock) {
                            disposePolling(deviceId);
                            closeSession(deviceId, sessionMap.remove(deviceId));
                        }
                    })
                    .subscribeOn(Schedulers.boundedElastic())
                    .then();
        }

        private void disposePolling(String deviceId) {
            Disposable disposable = pollingSubscriptionMap.remove(deviceId);
            if (disposable != null) {
                disposable.dispose();
                log.debug("Cancelled polling subscription for device {}", deviceId);
            }
        }

        private void closeSession(String deviceId, Plc4xDeviceSession session) {
            if (session == null) {
                return;
            }
            try {
                session.close()
                        .doOnError(e -> log.error("Error closing session for device {}: {}", deviceId, e.getMessage(), e))
                        .onErrorResume(e -> Mono.empty())
                        .block(SESSION_CLOSE_TIMEOUT);
            } catch (Exception e) {
                log.error("Error closing session for device {}: {}", deviceId, e.getMessage(), e);
            }
        }
    }

    /**
     * PLC4X Message Codec
     */
    @Slf4j
    static class Plc4xMessageCodec implements DeviceMessageCodec {

        private final Map<String, Plc4xDeviceSession> sessionMap;

        Plc4xMessageCodec(Map<String, Plc4xDeviceSession> sessionMap) {
            this.sessionMap = sessionMap;
        }

        @Override
        public Transport getSupportTransport() {
            return DefaultTransport.TCP;
        }

        @Nonnull
        @Override
        public Publisher<? extends Message> decode(@Nonnull MessageDecodeContext context) {
            return Mono.empty();
        }

        @Nonnull
        @Override
        public Publisher<? extends EncodedMessage> encode(@Nonnull MessageEncodeContext context) {
            Message message = context.getMessage();
            if (!(message instanceof DeviceMessage)) {
                return Mono.empty();
            }
            if (message instanceof ReadPropertyMessage) {
                return context
                        .reply(handleReadProperty((ReadPropertyMessage) message))
                        .then(Mono.empty());
            }
            if (message instanceof WritePropertyMessage) {
                return context
                        .reply(handleWriteProperty((WritePropertyMessage) message))
                        .then(Mono.empty());
            }
            if (message instanceof FunctionInvokeMessage) {
                FunctionInvokeMessage function = (FunctionInvokeMessage) message;
                return context
                        .reply(Mono.just(function.newReply()
                                .error(ErrorCode.UNSUPPORTED_MESSAGE.name(), "PLC4X function invoke is not supported")
                                .functionId(function.getFunctionId())))
                        .then(Mono.empty());
            }
            return Mono.empty();
        }

        private Mono<ReadPropertyMessageReply> handleReadProperty(ReadPropertyMessage message) {
            ReadPropertyMessageReply reply = message.newReply();
            Plc4xDeviceSession session = sessionMap.get(message.getDeviceId());
            if (session == null) {
                return Mono.just(reply.error(ErrorCode.SERVER_NOT_AVAILABLE.name(), "PLC4X session is not active"));
            }
            Mono<Map<String, Object>> properties = message.getProperties() == null || message.getProperties().isEmpty()
                    ? session.readAllPropertiesStrict()
                    : session.readPropertiesStrict(message.getProperties());

            return properties
                    .map(reply::success)
                    .switchIfEmpty(Mono.just(reply.error(ErrorCode.SERVER_NOT_AVAILABLE.name(), "PLC4X read returned no data")))
                    .onErrorResume(e -> Mono.just(reply.error(ErrorCode.of(e).name(), e.getMessage())));
        }

        private Mono<WritePropertyMessageReply> handleWriteProperty(WritePropertyMessage message) {
            WritePropertyMessageReply reply = message.newReply();
            Plc4xDeviceSession session = sessionMap.get(message.getDeviceId());
            if (session == null) {
                return Mono.just(reply.error(ErrorCode.SERVER_NOT_AVAILABLE.name(), "PLC4X session is not active"));
            }
            return session.writeProperties(message.getProperties())
                    .thenReturn(reply.success(message.getProperties()))
                    .onErrorResume(e -> Mono.just(reply.error(ErrorCode.of(e).name(), e.getMessage())));
        }
    }
}
