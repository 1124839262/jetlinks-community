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
package org.jetlinks.community.device.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 自定义设备属性写入MySQL的样例配置.
 *
 * @author jetlinks
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "device.message.writer.custom-mysql")
public class CustomMySqlDeviceMessageWriterProperties {

    private String url = "r2dbc:mysql://127.0.0.1:3306/jetlinks_custom?ssl=false&serverZoneId=Asia/Shanghai";

    private String username = "root";

    private String password = "";

    private String tableName = "plc_property_data";

    private String storageConfigKey = "customStorage";

    private String storageConfigValue = "mysql";

}
