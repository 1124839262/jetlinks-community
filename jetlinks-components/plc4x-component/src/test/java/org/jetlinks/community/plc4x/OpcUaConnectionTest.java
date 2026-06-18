package org.jetlinks.community.plc4x;

import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.PlcDriver;
import org.apache.plc4x.java.api.messages.PlcReadRequest;
import org.apache.plc4x.java.api.messages.PlcReadResponse;
import org.apache.plc4x.java.api.types.PlcResponseCode;

import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * OPC UA Connection Test for KEPServerEX - With connect() call
 */
public class OpcUaConnectionTest {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("PLC4X OPC UA Connection Test - With connect()");
        System.out.println("========================================\n");

        // 测试连接字符串 - 根据 PLC4X 0.12.0 文档
        String[] connectionStrings = {
            // 1. 启用 discovery（默认）
            "opcua:tcp://127.0.0.1:49320",

            // 2. 使用 localhost 替代 127.0.0.1
            "opcua:tcp://localhost:49320",

            // 3. 指定安全策略为 None
            "opcua:tcp://127.0.0.1:49320?security-policy=None",
        };

        try {
            // 1. 加载 OPC UA 驱动
            System.out.println("[1] Loading PLC4X drivers...");
            PlcDriver opcuaDriver = null;
            ServiceLoader<PlcDriver> loader = ServiceLoader.load(PlcDriver.class);

            int driverCount = 0;
            for (PlcDriver driver : loader) {
                String protocolCode = driver.getProtocolCode();
                if (protocolCode != null && !protocolCode.isEmpty()) {
                    driverCount++;
                    if ("opcua".equals(protocolCode)) {
                        opcuaDriver = driver;
                        System.out.println("    ✓ Found OPC UA driver: " + driver.getProtocolName());
                        System.out.println("    Driver class: " + driver.getClass().getName());
                    }
                }
            }
            System.out.println("    Total drivers: " + driverCount);

            if (opcuaDriver == null) {
                System.err.println("\n✗ OPC UA driver not found!");
                return;
            }

            // 2. 尝试每个连接字符串
            System.out.println("\n[2] Testing connections with connect()...\n");

            for (int i = 0; i < connectionStrings.length; i++) {
                String connectionString = connectionStrings[i];
                System.out.println("Test " + (i + 1) + ":");
                System.out.println("  URL: " + connectionString);

                try {
                    System.out.println("  Attempting connection...");
                    PlcConnection connection = opcuaDriver.getConnection(connectionString);

                    System.out.println("  Connection object: " + (connection != null ? connection.getClass().getName() : "null"));
                    System.out.println("  isConnected() before connect(): " + (connection != null ? connection.isConnected() : "N/A"));

                    if (connection != null) {
                        // 关键：调用 connect() 并等待完成
                        System.out.println("  Calling connect()...");
                        connection.connect();
                        // 给 OPC UA 握手一些时间
                        Thread.sleep(2000);

                        System.out.println("  isConnected() after connect(): " + connection.isConnected());

                        if (connection.isConnected()) {
                            System.out.println("\n  ✓✓✓ SUCCESS! ✓✓✓");
                            System.out.println("\n  Working connection string:");
                            System.out.println("  " + connectionString);
                            System.out.println("\n  Use this in JetLinks device configuration.");

                            // 实时读取标签 - KEPServer 模拟器示例下的 Ramp1
                            readTagContinuously(connection);

                            // 关闭连接
                            connection.close();
                            System.out.println("\n  Connection closed.");
                            return;
                        }
                    } else {
                        System.out.println("   Connection is null");
                    }
                } catch (Exception e) {
                    System.out.println("  ✗ Exception: " + e.getClass().getSimpleName());
                    System.out.println("    Message: " + e.getMessage());
                    if (e.getCause() != null) {
                        System.out.println("    Cause: " + e.getCause().getClass().getSimpleName() + ": " + e.getCause().getMessage());
                    }
                    // 打印完整堆栈
                    e.printStackTrace();
                }
                System.out.println();
            }

            System.out.println("\n========================================");
            System.out.println("ALL TESTS FAILED");
            System.out.println("========================================");
            System.out.println("\nPossible reasons:");
            System.out.println("1. PLC4X OPC UA driver has compatibility issues with KEPServerEX");
            System.out.println("2. Security policy mismatch");
            System.out.println("3. Authentication method not supported");
            System.out.println("\nSuggestions:");
            System.out.println("1. Check KEPServerEX OPC UA security settings");
            System.out.println("2. Try enabling Anonymous access in KEPServerEX");
            System.out.println("3. Use a different OPC UA client library");

        } catch (Exception e) {
            System.err.println("\n========================================");
            System.err.println("TEST FAILED");
            System.err.println("========================================");
            e.printStackTrace();
        }
    }

    /**
     * 读取 KEPServerEX 模拟器示例 Functions 下的 Ramp1 点位。
     *
     * <p>OPC UA 节点地址格式：{@code ns=<namespaceIndex>;s=<identifier>}。
     * KEPServerEX 中标签的 string identifier 即完整标签路径，
     * 模拟器示例下 Ramp1 的路径为 {@code Simulation Examples.Functions.Ramp1}，
     * 默认命名空间索引为 2。
     */
    private static void readTag(PlcConnection connection) {
        System.out.println("\n[3] Testing tag read...");

        if (!connection.getMetadata().isReadSupported()) {
            System.out.println("  ✗ This connection does not support reading.");
            return;
        }

        // PLC4X OPC UA 地址格式：ns=<index>;s=<identifier>
        // 节点标识符是 KEPServerEX 中标签的完整路径：<通道>.<设备/组>.<标签>
        // 本例通道名"模拟器示例"、组名"函数"均为中文，默认命名空间索引为 2，Ramp1 数据类型为 Long
        String tagName = "Ramp1";
        String address = "ns=2;s=模拟器示例.函数.Ramp1";

        System.out.println("  Tag name: " + tagName);
        System.out.println("  Address : " + address);

        try {
            // 构建读取请求
            PlcReadRequest readRequest = connection
                .readRequestBuilder()
                .addTagAddress(tagName, address)
                .build();

            // 发起异步读取并等待结果
            CompletableFuture<? extends PlcReadResponse> future = readRequest.execute();
            PlcReadResponse response = future.get(5, TimeUnit.SECONDS);

            PlcResponseCode code = response.getResponseCode(tagName);
            System.out.println("  Response code: " + code);

            if (code == PlcResponseCode.OK) {
                Object value = response.getObject(tagName);
                System.out.println("\n  ✓✓✓ Read SUCCESS ✓✓✓");
                System.out.println("  " + tagName + " = " + value);
            } else {
                System.out.println("  ✗ Read failed with code: " + code);
            }
        } catch (Exception e) {
            System.out.println("  ✗ Read exception: " + e.getClass().getSimpleName());
            System.out.println("    Message: " + e.getMessage());
            if (e.getCause() != null) {
                System.out.println("    Cause: " + e.getCause().getClass().getSimpleName()
                    + ": " + e.getCause().getMessage());
            }
        }
    }

    /**
     * 实时读取 Ramp1 点位，每秒打印一次当前值。按 Ctrl+C 停止。
     */
    private static void readTagContinuously(PlcConnection connection) {
        System.out.println("\n[3] Real-time tag monitoring...");
        System.out.println("  Press Ctrl+C to stop\n");

        if (!connection.getMetadata().isReadSupported()) {
            System.out.println("  ✗ This connection does not support reading.");
            return;
        }

        String tagName = "Ramp1";
        String address = "ns=2;s=模拟器示例.函数.Ramp1";

        System.out.println("  Tag name: " + tagName);
        System.out.println("  Address : " + address);
        System.out.println("  ----------------------------------------");

        int readCount = 0;
        while (true) {
            try {
                PlcReadRequest readRequest = connection
                    .readRequestBuilder()
                    .addTagAddress(tagName, address)
                    .build();

                CompletableFuture<? extends PlcReadResponse> future = readRequest.execute();
                PlcReadResponse response = future.get(5, TimeUnit.SECONDS);

                PlcResponseCode code = response.getResponseCode(tagName);
                readCount++;

                if (code == PlcResponseCode.OK) {
                    Object value = response.getObject(tagName);
                    String timestamp = java.time.LocalDateTime.now()
                        .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
                    System.out.printf("  [%s] #%-4d %s = %s%n", timestamp, readCount, tagName, value);
                } else {
                    System.out.println("  ✗ Read failed: " + code);
                }

                // 每秒读取一次
                Thread.sleep(1000);

            } catch (InterruptedException e) {
                System.out.println("\n  Interrupted, stopping...");
                break;
            } catch (Exception e) {
                System.out.println("  ✗ Exception: " + e.getMessage());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    break;
                }
            }
        }
    }
}