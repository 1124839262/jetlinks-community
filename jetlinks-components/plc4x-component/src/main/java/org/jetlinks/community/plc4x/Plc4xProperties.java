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

import java.util.Collections;
import java.util.List;

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

    /**
     * 是否启用设备分片。
     *
     * <p>启用后，只有设备的归属节点（基于设备ID哈希取模计算）才会建立 PLC 采集会话，
     * 避免多节点重复连接同一 OPC UA Server。
     */
    private boolean shardingEnabled = false;

    /**
     * 当前节点ID，用于分片判断。
     *
     * <p>通常配置为 {@code ${jetlinks.cluster.id}}，引用集群节点ID。
     */
    private String currentNodeId;

    /**
     * 集群所有节点ID列表，由 Plc4xAutoConfiguration 在启动时从 ClusterManager 自动填充。
     */
    private List<String> allNodeIds = Collections.emptyList();
}