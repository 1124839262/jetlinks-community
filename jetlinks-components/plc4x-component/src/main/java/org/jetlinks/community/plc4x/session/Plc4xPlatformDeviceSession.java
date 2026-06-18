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
package org.jetlinks.community.plc4x.session;

import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.exception.DeviceOperationException;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.server.session.DeviceSession;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Optional;

/**
 * PLC4X 平台设备会话。
 *
 * <p>用于在 JetLinks {@code DeviceSessionManager} 中标识 PLC 设备的在线状态。
 * 与 {@link Plc4xDeviceSession}（负责 PLC 数据采集）不同，本类只承担平台侧会话职责：
 * 维持在线状态、保活超时管理。
 *
 * <p>PLC 设备的下行写入通过 {@code Plc4xConnectionManager} 完成，不经过会话的
 * {@link #send(EncodedMessage)}，因此 send 直接返回不支持。
 */
public class Plc4xPlatformDeviceSession implements DeviceSession {

    private final DeviceOperator operator;

    private final long connectTime = System.currentTimeMillis();

    private volatile long lastPingTime = System.currentTimeMillis();

    // 默认永不超时，由轮询上报持续刷新保活
    private volatile long keepAliveTimeOutMs = -1;

    private Runnable onCloseCallback;

    public Plc4xPlatformDeviceSession(DeviceOperator operator) {
        this.operator = operator;
    }

    @Override
    public String getId() {
        return operator.getDeviceId();
    }

    @Override
    public String getDeviceId() {
        return operator.getDeviceId();
    }

    @Nullable
    @Override
    public DeviceOperator getOperator() {
        return operator;
    }

    @Override
    public long lastPingTime() {
        return lastPingTime;
    }

    @Override
    public long connectTime() {
        return connectTime;
    }

    @Override
    public Mono<Boolean> send(EncodedMessage encodedMessage) {
        // PLC 下行写入走 Plc4xConnectionManager，不经过会话发送通道
        return Mono.error(new DeviceOperationException.NoStackTrace(ErrorCode.UNSUPPORTED_MESSAGE));
    }

    @Override
    public Transport getTransport() {
        return DefaultTransport.TCP;
    }

    @Override
    public Optional<InetSocketAddress> getClientAddress() {
        return Optional.empty();
    }

    @Override
    public void close() {
        if (onCloseCallback != null) {
            onCloseCallback.run();
        }
    }

    @Override
    public void setKeepAliveTimeout(Duration timeout) {
        keepAliveTimeOutMs = timeout.toMillis();
    }

    @Override
    public void ping() {
        lastPingTime = System.currentTimeMillis();
    }

    @Override
    public boolean isAlive() {
        return keepAliveTimeOutMs <= 0
            || System.currentTimeMillis() - lastPingTime < keepAliveTimeOutMs;
    }

    @Override
    public void onClose(Runnable call) {
        this.onCloseCallback = call;
    }
}
