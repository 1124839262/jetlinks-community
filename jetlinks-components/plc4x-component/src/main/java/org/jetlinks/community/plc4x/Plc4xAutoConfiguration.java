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

import org.jetlinks.community.gateway.DeviceGatewayHelper;
import org.jetlinks.community.plc4x.protocol.Plc4xBridgeProtocolSupportProvider;
import org.jetlinks.community.protocol.CommandSupportServiceContext;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.cluster.ServerNode;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.supports.server.DecodedClientMessageHandler;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import java.util.List;
import java.util.stream.Collectors;

/**
 * PLC4X Component Auto Configuration
 */
@AutoConfiguration
@ComponentScan(basePackages = "org.jetlinks.community.plc4x")
@EnableConfigurationProperties(Plc4xProperties.class)
public class Plc4xAutoConfiguration {

    @Bean
    public ProtocolSupport plc4xBridgeProtocolSupport(DeviceRegistry deviceRegistry,
                                                      DeviceSessionManager deviceSessionManager,
                                                      DecodedClientMessageHandler clientMessageHandler,
                                                      Plc4xProperties properties,
                                                      ObjectProvider<ClusterManager> clusterManagerProvider) {
        // 启用分片时，从 ClusterManager 获取集群所有节点ID列表
        ClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        if (clusterManager != null && properties.isShardingEnabled()) {
            List<String> nodeIds = clusterManager
                    .getHaManager()
                    .getAllNode()
                    .stream()
                    .map(ServerNode::getId)
                    .sorted()
                    .collect(Collectors.toList());
            properties.setAllNodeIds(nodeIds);
        }

        // 构造网关助手，使 PLC 轮询数据可以上报到平台（设备上线 + 属性入库）
        DeviceGatewayHelper gatewayHelper = new DeviceGatewayHelper(
                deviceRegistry, deviceSessionManager, clientMessageHandler);

        Plc4xBridgeProtocolSupportProvider provider = new Plc4xBridgeProtocolSupportProvider(gatewayHelper, properties);
        return provider.create(CommandSupportServiceContext.INSTANCE).block();
    }
}