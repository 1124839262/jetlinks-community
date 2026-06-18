package org.jetlinks.community.plc4x.config;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.community.plc4x.util.Plc4xConnectionStringUtils;

/**
 * Device configuration for PLC4X connection
 */
@Getter
@Setter
public class Plc4xDeviceConfig {

    /**
     * PLC4X connection string
     * <p>
     * 连接字符串格式: {protocol}:{transport}://{host}:{port}?{parameters}
     * <p>
     * 示例:
     * <ul>
     *   <li>S7: s7://192.168.1.10?remote-rack=0&remote-slot=1</li>
     *   <li>Modbus: modbus-tcp://192.168.1.20:502</li>
     *   <li>OPC UA (匿名): opcua:tcp://127.0.0.1:49320</li>
     *   <li>OPC UA (密码): opcua:tcp://127.0.0.1:49320?username=admin&password=pass123</li>
     *   <li>OPC UA (加密): opcua:tcp://127.0.0.1:49320?username=admin&password=pass123&security-policy=Basic256Sha256</li>
     * </ul>
     * <p>
     * 注意: 建议使用 username 和 password 字段分离配置认证信息，避免密码明文出现在连接字符串中
     * <p>
     * 文档: <a href="https://plc4x.apache.org/plc4x/0.13.0/users/protocols/">PLC4X Protocol Documentation</a>
     */
    private String connectionString;

    /**
     * Username for authentication (optional, overrides username in connection string)
     * 认证用户名（可选，优先级高于连接字符串中的 username 参数）
     */
    private String username;

    /**
     * Password for authentication (optional, overrides password in connection string)
     * 认证密码（可选，优先级高于连接字符串中的 password 参数）
     */
    private String password;

    /**
     * Polling interval in milliseconds
     */
    private Long interval;

    /**
     * Request timeout in milliseconds
     */
    private Long timeout;

    /**
     * Auto subscribe to thing model properties
     */
    private Boolean autoSubscribe;

    /**
     * Build final connection string with username and password
     */
    public String buildConnectionString() {
        return Plc4xConnectionStringUtils.withAuthentication(connectionString, username, password);
    }
}
