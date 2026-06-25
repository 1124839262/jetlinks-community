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
package org.jetlinks.community.standalone.plc4x;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.device.entity.DeviceInstanceEntity;
import org.jetlinks.community.device.entity.DeviceProductEntity;
import org.jetlinks.community.device.enums.DeviceState;
import org.jetlinks.community.device.service.LocalDeviceInstanceService;
import org.jetlinks.community.device.service.LocalDeviceProductService;
import org.jetlinks.community.device.web.response.DeviceDeployResult;
import org.jetlinks.community.plc4x.Plc4xProperties;
import org.jetlinks.community.plc4x.sharding.Plc4xDeviceSharding;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;

/**
 * PLC4X 设备重启恢复。
 *
 * <p>PLC 设备的采集会话由协议的 {@code onDeviceRegister} 回调建立，且只存在于内存中。
 * 后端重启后，已激活的存量设备不会自动重新触发注册，导致采集会话丢失、设备显示离线。
 *
 * <p>本组件在应用就绪后，查询所有使用 plc4x 协议的已激活设备，重新执行
 * {@code deploy} 触发注册流程，从而重建 PLC 采集会话，无需手动禁用/启用。
 *
 * <p>放置在 standalone 组装层而非 plc4x-component，是为了避免协议组件反向依赖
 * device-manager 的查询服务（保持组件分层）。
 */
@Component
@Slf4j
@AllArgsConstructor
public class Plc4xDeviceRecoveryRunner implements ApplicationRunner {

    private static final String PLC4X_PROTOCOL_ID = "plc4x-bridge";

    private final LocalDeviceInstanceService deviceService;

    private final LocalDeviceProductService productService;

    private final Plc4xProperties plc4xProperties;

    @Override
    public void run(ApplicationArguments args) {
        recoverPlcDevices()
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        count -> {
                            if (count > 0) {
                                log.info("PLC4X device recovery completed, redeployed {} device(s)", count);
                            }
                        },
                        err -> log.error("PLC4X device recovery failed", err)
                );
    }

    private Mono<Long> recoverPlcDevices() {
        return findPlc4xProductIds()
                .collectList()
                .flatMap(productIds -> {
                    if (productIds.isEmpty()) {
                        return Mono.just(0L);
                    }
                    return redeployActivatedDevices(productIds);
                });
    }

    private Flux<String> findPlc4xProductIds() {
        return productService
                .createQuery()
                .where(DeviceProductEntity::getMessageProtocol, PLC4X_PROTOCOL_ID)
                .fetch()
                .map(DeviceProductEntity::getId);
    }

    private Mono<Long> redeployActivatedDevices(List<String> productIds) {
        Flux<DeviceInstanceEntity> devices = deviceService
                .createQuery()
                .where()
                .in(DeviceInstanceEntity::getProductId, productIds)
                // 只恢复已激活的设备（在线/离线），禁用(notActive)的设备保持禁用
                .not(DeviceInstanceEntity::getState, DeviceState.notActive)
                .fetch();

        return devices
                .filter(device -> {
                    // 分片过滤：只恢复属于当前节点的设备
                    if (plc4xProperties.isShardingEnabled() && !plc4xProperties.getAllNodeIds().isEmpty()) {
                        boolean isOwner = Plc4xDeviceSharding.isOwner(
                                device.getId(),
                                plc4xProperties.getCurrentNodeId(),
                                plc4xProperties.getAllNodeIds());
                        if (!isOwner) {
                            log.debug("Device {} belongs to another node, skip recovery", device.getId());
                            return false;
                        }
                    }
                    return true;
                })
                .collectList()
                .flatMap(list -> {
                    if (list.isEmpty()) {
                        return Mono.just(0L);
                    }
                    log.info("Recovering {} PLC4X device(s) after restart (sharding: {}, node: {})...",
                            list.size(),
                            plc4xProperties.isShardingEnabled(),
                            plc4xProperties.getCurrentNodeId());
                    return deviceService
                            .deploy(Flux.fromIterable(list))
                            .doOnNext(result -> {
                                if (!result.isSuccess()) {
                                    log.warn("PLC4X device recovery deploy failed: {}", result.getMessage());
                                }
                            })
                            .filter(DeviceDeployResult::isSuccess)
                            .map(result -> (long) result.getTotal())
                            .reduce(0L, Long::sum);
                });
    }
}