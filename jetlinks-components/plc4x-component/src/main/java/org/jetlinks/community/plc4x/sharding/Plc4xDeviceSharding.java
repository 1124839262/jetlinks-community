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
package org.jetlinks.community.plc4x.sharding;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * PLC4X 设备分片工具。
 *
 * <p>基于设备ID哈希取模，将设备均匀分配到集群各节点。
 * 每个设备只有一个归属节点，只有归属节点才会建立 PLC 采集会话，
 * 避免多节点重复连接同一 OPC UA Server 导致的连接数翻倍和数据重复采集。
 *
 * <p>分片算法：对节点ID列表排序后，{@code ownerIndex = floorMod(deviceId.hashCode(), nodeCount)}，
 * 取排序后第 ownerIndex 个节点作为归属节点。排序是为了保证所有节点计算结果一致。
 */
public final class Plc4xDeviceSharding {

    private Plc4xDeviceSharding() {
    }

    /**
     * 计算设备的归属节点ID。
     *
     * @param deviceId 设备ID
     * @param nodeIds  集群所有节点ID列表（无需预先排序，方法内部会排序）
     * @return 归属节点ID；如果节点列表为空则返回 null
     */
    public static String getOwnerNode(String deviceId, List<String> nodeIds) {
        if (deviceId == null || nodeIds == null || nodeIds.isEmpty()) {
            return null;
        }
        List<String> sorted = nodeIds
                .stream()
                .sorted()
                .collect(Collectors.toList());
        if (sorted.size() == 1) {
            return sorted.get(0);
        }
        int index = Math.floorMod(deviceId.hashCode(), sorted.size());
        return sorted.get(index);
    }

    /**
     * 判断当前节点是否是指定设备的归属节点。
     *
     * <p>当未配置当前节点ID或节点列表为空时，默认返回 true（即不做分片，所有节点都处理），
     * 保证单节点部署或未启用分片时功能正常。
     *
     * @param deviceId      设备ID
     * @param currentNodeId 当前节点ID
     * @param nodeIds       集群所有节点ID列表
     * @return true 表示当前节点是归属节点，应处理该设备
     */
    public static boolean isOwner(String deviceId, String currentNodeId, List<String> nodeIds) {
        if (currentNodeId == null || currentNodeId.isEmpty()) {
            return true;
        }
        String owner = getOwnerNode(deviceId, nodeIds);
        if (owner == null) {
            return true;
        }
        return currentNodeId.equals(owner);
    }
}