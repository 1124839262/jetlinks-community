package org.jetlinks.community.plc4x.connection;

import lombok.extern.slf4j.Slf4j;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.PlcDriver;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * PLC4X Connection Manager with connection pooling, health check and auto-reconnect support
 */
@Slf4j
public class Plc4xConnectionManager {

    private final Map<String, PlcConnection> connectionPool = new ConcurrentHashMap<>();
    private final Map<String, PlcDriver> driverCache = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> connectionLocks = new ConcurrentHashMap<>();
    private final Map<String, String> connectionStringCache = new ConcurrentHashMap<>();

    private final ScheduledExecutorService healthCheckExecutor;
    private final int maxPoolSize;
    private final boolean healthCheckEnabled;

    private volatile boolean shutdown = false;

    public Plc4xConnectionManager() {
        this(50, true, 30);
    }

    public Plc4xConnectionManager(int maxPoolSize, boolean healthCheckEnabled, long healthCheckIntervalSeconds) {
        this.maxPoolSize = maxPoolSize;
        this.healthCheckEnabled = healthCheckEnabled;

        // 加载 PLC4X 驱动
        ServiceLoader.load(PlcDriver.class).forEach(driver -> {
            String protocolCode = driver.getProtocolCode();
            if (protocolCode != null && !protocolCode.isEmpty()) {
                driverCache.put(protocolCode, driver);
                //log.info("Loaded PLC4X driver: {} (protocol: {})", driver.getProtocolName(), protocolCode);
            }
        });
        //log.info("Total PLC4X drivers loaded: {}", driverCache.size());

        // 启动健康检查
        if (healthCheckEnabled) {
            this.healthCheckExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "plc4x-health-check");
                thread.setDaemon(true);
                return thread;
            });
            this.healthCheckExecutor.scheduleWithFixedDelay(
                this::performHealthCheck,
                healthCheckIntervalSeconds,
                healthCheckIntervalSeconds,
                TimeUnit.SECONDS
            );
            //log.info("PLC4X health check enabled, interval: {}s", healthCheckIntervalSeconds);
        } else {
            this.healthCheckExecutor = null;
            //log.info("PLC4X health check disabled");
        }
    }

    /**
     * Get or create a PLC connection with retry
     *
     * @param connectionId unique connection identifier (usually device ID)
     * @param connectionString PLC4X connection string
     * @return Mono with the connection
     */
    public Mono<PlcConnection> getConnection(String connectionId, String connectionString) {
        return Mono.defer(() -> {
            // 验证连接字符串格式
            String normalizedConnectionString = validateConnectionString(connectionString);

            // 检查连接池大小限制
            if (connectionPool.size() >= maxPoolSize && !connectionPool.containsKey(connectionId)) {
                return Mono.error(new PlcConnectionException(
                    "Connection pool is full (max: " + maxPoolSize + "), cannot create new connection"));
            }

            PlcConnection existing = connectionPool.get(connectionId);
            if (existing != null && existing.isConnected()) {
                //log.debug("Reusing existing connection for: {}", connectionId);
                return Mono.just(existing);
            }

            // 缓存连接字符串用于重连
            connectionStringCache.put(connectionId, normalizedConnectionString);

            return createConnectionWithRetry(connectionId, normalizedConnectionString);
        });
    }

    /**
     * Create connection with lock to prevent race conditions
     */
    private Mono<PlcConnection> createConnectionWithLock(String connectionId, String connectionString) {
        ReentrantLock lock = connectionLocks.computeIfAbsent(connectionId, k -> new ReentrantLock());
        return Mono.fromCallable(() -> {
            lock.lock();
            try {
                PlcConnection existing = connectionPool.get(connectionId);
                if (existing != null && existing.isConnected()) {
                    log.debug("Connection created by another thread, reusing for: {}", connectionId);
                    return existing;
                }
                if (existing != null) {
                    closeQuietly(existing);
                    connectionPool.remove(connectionId, existing);
                }
                return createConnectionNow(connectionId, connectionString);
            } finally {
                lock.unlock();
                connectionLocks.remove(connectionId, lock);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Create a new PLC connection with retry mechanism
     */
    private Mono<PlcConnection> createConnectionWithRetry(String connectionId, String connectionString) {
        return createConnectionWithLock(connectionId, connectionString)
            .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                .maxBackoff(Duration.ofSeconds(10))
                .filter(e -> e instanceof PlcConnectionException)
                .doBeforeRetry(signal ->
                    log.warn("Retrying connection for {} (attempt {}/3): {}",
                        connectionId, signal.totalRetries() + 1, signal.failure().getMessage()))
            )
            .onErrorResume(e -> {
                log.error("Failed to create PLC connection for {} after retries: {}", connectionId, e.getMessage(), e);
                return Mono.error(new PlcConnectionException("Failed to connect to PLC after 3 retries: " + e.getMessage(), e));
            });
    }

    /**
     * Create a new PLC connection
     */
    private PlcConnection createConnectionNow(String connectionId, String connectionString) throws PlcConnectionException {
        log.info("Creating new PLC connection for: {} with connectionString: {}",
                connectionId, maskConnectionString(connectionString));

        // Parse connection string to get protocol code (e.g., "s7" from "s7://192.168.1.10")
        String protocolCode = parseProtocolCode(connectionString);

        PlcDriver driver = driverCache.get(protocolCode);
        if (driver == null) {
            throw new PlcConnectionException("No driver found for protocol: " + protocolCode);
        }

        PlcConnection connection = driver.getConnection(connectionString);

        log.debug("Calling connect() for connection: {}", connectionId);
        // 调用 connect() 建立实际连接（OPC UA 必需）
        connection.connect();

        // OPC UA 需要时间完成握手，等待 2 秒确保连接建立
        if ("opcua".equalsIgnoreCase(protocolCode)) {
            log.debug("Waiting for OPC UA handshake to complete...");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PlcConnectionException("Interrupted while waiting for OPC UA handshake", e);
            }
        }

        // 验证连接状态
        if (!connection.isConnected()) {
            closeQuietly(connection);
            throw new PlcConnectionException("Connection established but isConnected() returns false");
        }

        connectionPool.put(connectionId, connection);
        log.info("Successfully connected to PLC: {} (isConnected: {})", connectionId, connection.isConnected());
        return connection;
    }

    /**
     * Parse protocol code from connection string
     * e.g., "s7://192.168.1.10" -> "s7"
     */
    private String parseProtocolCode(String connectionString) {
        int colonIndex = connectionString.indexOf(':');
        if (colonIndex > 0) {
            return connectionString.substring(0, colonIndex).toLowerCase(Locale.ROOT);
        }
        throw new IllegalArgumentException("Invalid connection string format: " + connectionString);
    }

    /**
     * Validate connection string format
     */
    private String validateConnectionString(String connectionString) {
        if (connectionString == null || connectionString.trim().isEmpty()) {
            throw new IllegalArgumentException("Connection string cannot be null or empty");
        }

        String trimmed = connectionString.trim();
        String lowerCase = trimmed.toLowerCase(Locale.ROOT);

        // 基本格式验证: protocol://host 或 protocol:transport://host
        if (!lowerCase.matches("^[a-z][a-z0-9+\\.\\-]*(?:://|:[a-z][a-z0-9+\\.\\-]*://).+")) {
            throw new IllegalArgumentException(
                "Invalid connection string format. Expected: {protocol}://{host} or {protocol}:{transport}://{host}:{port}?{parameters}\n" +
                "Examples:\n" +
                "  - s7://192.168.1.10\n" +
                "  - opcua:tcp://127.0.0.1:49320\n" +
                "  - modbus-tcp://192.168.1.20:502"
            );
        }
        return trimmed;
    }

    /**
     * Perform health check on all connections
     */
    private void performHealthCheck() {
        if (shutdown) {
            return;
        }

        log.debug("Performing health check on {} connections", connectionPool.size());

        connectionPool.forEach((connectionId, connection) -> {
            try {
                if (!connection.isConnected()) {
                    log.warn("Connection {} is disconnected, attempting reconnect", connectionId);
                    reconnect(connectionId);
                }
            } catch (Exception e) {
                log.error("Health check failed for connection {}: {}", connectionId, e.getMessage());
            }
        });
    }

    /**
     * Reconnect a disconnected connection
     */
    private void reconnect(String connectionId) {
        String connectionString = connectionStringCache.get(connectionId);
        if (connectionString == null) {
            log.error("Cannot reconnect {}: connection string not found in cache", connectionId);
            return;
        }

        // 移除旧连接
        PlcConnection oldConnection = connectionPool.remove(connectionId);
        if (oldConnection != null) {
            closeQuietly(oldConnection);
        }

        // 创建新连接
        createConnectionWithRetry(connectionId, connectionString)
            .subscribe(
                conn -> log.info("Successfully reconnected: {}", connectionId),
                error -> log.error("Failed to reconnect {}: {}", connectionId, error.getMessage())
            );
    }

    /**
     * Close and remove a connection from the pool
     */
    public Mono<Void> closeConnection(String connectionId) {
        return Mono.fromRunnable(() -> {
            connectionStringCache.remove(connectionId);
            PlcConnection connection = connectionPool.remove(connectionId);
            if (connection != null) {
                closeQuietly(connection);
                log.info("Closed PLC connection: {}", connectionId);
            }
        });
    }

    /**
     * Check if a connection is healthy
     */
    public Mono<Boolean> isConnectionHealthy(String connectionId) {
        return Mono.fromSupplier(() -> {
            PlcConnection connection = connectionPool.get(connectionId);
            return connection != null && connection.isConnected();
        });
    }

    /**
     * Get the number of active connections
     */
    public int getActiveConnectionCount() {
        return (int) connectionPool.values().stream()
            .filter(PlcConnection::isConnected)
            .count();
    }

    /**
     * Close all connections
     */
    @PreDestroy
    public Mono<Void> closeAll() {
        return Mono.fromRunnable(() -> {
            shutdown = true;

            log.info("Closing all PLC connections, total: {}", connectionPool.size());

            if (healthCheckExecutor != null) {
                healthCheckExecutor.shutdown();
                try {
                    if (!healthCheckExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        healthCheckExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    healthCheckExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }

            connectionPool.forEach((id, conn) -> {
                try {
                    closeQuietly(conn);
                } catch (Exception e) {
                    log.error("Error closing connection {}: {}", id, e.getMessage());
                }
            });
            connectionPool.clear();
            connectionStringCache.clear();
        });
    }

    private void closeQuietly(PlcConnection connection) {
        try {
            if (connection.isConnected()) {
                connection.close();
            }
        } catch (Exception e) {
            log.debug("Error closing PLC connection: {}", e.getMessage());
        }
    }

    private String maskConnectionString(String connectionString) {
        return connectionString
            .replaceAll("(?i)([?&](?:username|password)=)[^&]*", "$1******")
            .replaceAll("(?i)(//[^/:@?]+:)[^/@?]*@", "$1******@");
    }
}
