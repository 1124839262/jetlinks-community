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
package org.jetlinks.community.plc4x;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * PLC4X component configuration properties
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "jetlinks.plc4x")
public class Plc4xProperties {

    /**
     * Default polling interval in milliseconds
     */
    private long defaultInterval = 5000;

    /**
     * Default timeout in milliseconds
     */
    private long defaultTimeout = 10000;

    /**
     * Enable auto subscribe
     */
    private boolean autoSubscribe = true;

    /**
     * Max connection pool size per protocol
     */
    private int maxPoolSize = 50;

    /**
     * Enable connection health check
     */
    private boolean healthCheckEnabled = true;

    /**
     * Health check interval in seconds
     */
    private long healthCheckInterval = 30;
}
