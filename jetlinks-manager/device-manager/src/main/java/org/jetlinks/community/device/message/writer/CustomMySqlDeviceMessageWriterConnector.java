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
package org.jetlinks.community.device.message.writer;

import io.r2dbc.spi.ConnectionFactory;
import lombok.Generated;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.device.configuration.CustomMySqlDeviceMessageWriterProperties;
import org.jetlinks.community.gateway.annotation.Subscribe;
import org.jetlinks.core.Value;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.property.Property;
import org.jetlinks.core.message.property.PropertyMessage;
import org.springframework.boot.r2dbc.ConnectionFactoryBuilder;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 自定义写MySQL的设备属性样例.
 *
 * @author jetlinks
 */
@Slf4j
public class CustomMySqlDeviceMessageWriterConnector {

    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("[A-Za-z0-9_]+");

    private final DeviceRegistry registry;

    private final CustomMySqlDeviceMessageWriterProperties properties;

    private final DatabaseClient databaseClient;

    private final String tableName;

    public CustomMySqlDeviceMessageWriterConnector(DeviceRegistry registry,
                                                   CustomMySqlDeviceMessageWriterProperties properties) {
        this.registry = registry;
        this.properties = properties;
        this.tableName = validateTableName(properties.getTableName());
        this.databaseClient = DatabaseClient.create(createConnectionFactory(properties));
    }

    @Subscribe(topics = "/device/*/*/message/property/report", id = "custom-mysql-device-message-writer", priority = 90)
    @Generated
    public Mono<Void> writeDevicePropertyToMySql(DeviceMessage message) {
        if (!(message instanceof PropertyMessage)) {
            return Mono.empty();
        }

        PropertyMessage propertyMessage = ((PropertyMessage) message);
        return isMySqlStorage(message.getDeviceId())
            .filter(Boolean::booleanValue)
            .flatMapMany(ignore -> Flux.fromIterable(propertyMessage.getCompleteProperties()))
            .concatMap(property -> writeProperty(message, property))
            .then()
            .onErrorResume(err -> {
                log.warn("write custom mysql device property failed, deviceId: {}", message.getDeviceId(), err);
                return Mono.empty();
            });
    }

    private Mono<Boolean> isMySqlStorage(String deviceId) {
        if (!StringUtils.hasText(deviceId)) {
            return Mono.just(false);
        }
        return registry
            .getDevice(deviceId)
            .flatMap(device -> device.getConfig(properties.getStorageConfigKey()))
            .map(Value::asString)
            .map(value -> Objects.equals(properties.getStorageConfigValue(), value))
            .defaultIfEmpty(false);
    }

    private Mono<Void> writeProperty(DeviceMessage message, Property property) {
        Object value = property.getValue();
        Number numberValue = value instanceof Number ? ((Number) value) : null;
        long timestamp = property.getTimestamp() > 0 ? property.getTimestamp() : message.getTimestamp();

        DatabaseClient.GenericExecuteSpec spec = databaseClient
            .sql("insert into " + tableName + " (" +
                     "device_id, property_id, value_text, number_value, report_time" +
                 ") values (:deviceId, :propertyId, :valueText, :numberValue, :reportTime)")
            .bind("deviceId", message.getDeviceId())
            .bind("propertyId", property.getProperty())
            .bind("valueText", String.valueOf(value))
            .bind("reportTime", timestamp);

        if (numberValue == null) {
            spec = spec.bindNull("numberValue", Double.class);
        } else {
            spec = spec.bind("numberValue", numberValue.doubleValue());
        }

        return spec
            .fetch()
            .rowsUpdated()
            .then();
    }

    private static ConnectionFactory createConnectionFactory(CustomMySqlDeviceMessageWriterProperties properties) {
        ConnectionFactoryBuilder builder = ConnectionFactoryBuilder.withUrl(properties.getUrl());
        if (StringUtils.hasText(properties.getUsername())) {
            builder.username(properties.getUsername());
        }
        if (StringUtils.hasText(properties.getPassword())) {
            builder.password(properties.getPassword());
        }
        return builder.build();
    }

    private static String validateTableName(String tableName) {
        if (!StringUtils.hasText(tableName) || !TABLE_NAME_PATTERN.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid custom mysql table name: " + tableName);
        }
        return tableName;
    }

}
