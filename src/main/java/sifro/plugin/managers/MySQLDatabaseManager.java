package sifro.plugin.managers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import sifro.plugin.config.MySQLConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.*;

/**
 * MySQL manager implementation using HikariCP.
 *
 * <p>Keeps the API async by executing work on a cached thread pool and acquiring
 * connections from a Hikari pool.</p>
 *
 * <p><b>Security warning:</b> {@link #executeAsync(String)} is dangerous if used with untrusted input.
 * Prefer prepared statement IDs via {@link #prepare(String)}.</p>
 *
 * </p><br>For more info see {@link DatabaseManager}.</p></p>
 */
public final class MySQLDatabaseManager extends DatabaseManager {

    /**
     * Hikari datasource for MySQL connections.
     */
    private final HikariDataSource hikariDataSource;

    /**
     * Executor used for both reads and writes.
     */
    private final ExecutorService executor;

    /**
     * The supplied config.
     */
    private final MySQLConfig config;

    /**
     * Creates a MySQLDatabaseManager using the provided config.
     *
     * @param conf MySQL configuration
     */
    public MySQLDatabaseManager(MySQLConfig conf) {
        this.config = conf;
        this.executor = Executors.newCachedThreadPool();
        this.hikariDataSource = createDataSource();
    }

    /**
     * Builds and configures a Hikari datasource for MySQL.
     *
     * @return configured datasource
     */
    private HikariDataSource createDataSource() {
        HikariConfig cfg = new HikariConfig();

        String jdbcUrl = String.format(
                "jdbc:mysql://%s:%s/%s",
                config.getHost(),
                config.getPort(),
                config.getDatabase()
        );

        cfg.setJdbcUrl(jdbcUrl);
        cfg.setUsername(config.getUsername());
        cfg.setPassword(config.getPassword());

        // Pool sizing
        cfg.setMaximumPoolSize(Math.max(1, config.getPoolSize()));
        cfg.setMinimumIdle(Math.min(2, Math.max(0, config.getPoolSize())));

        // Timeouts
        cfg.setConnectionTimeout(5_000); // 5 seconds
        cfg.setValidationTimeout(2_000); // 2 seconds

        // Lifetime tuning
        cfg.setIdleTimeout(60_000); // 1 minute
        cfg.setMaxLifetime(30 * 60_000); // 30 minutes

        cfg.setKeepaliveTime(0);

        cfg.setAutoCommit(true);
        cfg.setPoolName("SQL-mysql-" + Integer.toHexString(System.identityHashCode(config)));

        // Connector/J performance properties
        cfg.addDataSourceProperty("cachePrepStmts", "true");
        cfg.addDataSourceProperty("prepStmtCacheSize", "500");
        cfg.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        cfg.addDataSourceProperty("useServerPrepStmts", "true");

        cfg.addDataSourceProperty("useLocalSessionState", "true");
        cfg.addDataSourceProperty("rewriteBatchedStatements", "true");
        cfg.addDataSourceProperty("cacheResultSetMetadata", "true");
        cfg.addDataSourceProperty("cacheServerConfiguration", "true");
        cfg.addDataSourceProperty("elideSetAutoCommits", "true");
        cfg.addDataSourceProperty("maintainTimeStats", "false");

        return new HikariDataSource(cfg);
    }

    /**
     * Returns a pooled connection from Hikari.
     *
     * @param write whether the caller intends to write (unused for MySQL)
     * @return pooled connection
     * @throws SQLException if pool cannot provide a connection
     */
    @Override
    protected Connection getConnection(boolean write) throws SQLException {
        return hikariDataSource.getConnection();
    }

    /**
     * Releases a pooled connection back to Hikari.
     *
     * <p>In Hikari, calling {@link Connection#close()} returns the connection to the pool.</p>
     *
     * @param connection connection to release
     * @throws SQLException if close fails
     */
    @Override
    protected void closeConnection(Connection connection) throws SQLException {
        connection.close();
    }

    /**
     * @return executor for writes
     */
    @Override
    protected ExecutorService writeExecutor() {
        return executor;
    }

    /**
     * @return executor for reads
     */
    @Override
    protected ExecutorService readExecutor() {
        return executor;
    }

    /**
     * Closes the MySQL manager.
     *
     * <p>Shutdown sequence:
     * <ol>
     *   <li>close Hikari datasource</li>
     *   <li>shutdown executors (max. 5sec wait)</li>
     * </ol>
     * </p>
     */
    @Override
    public void close() {
        hikariDataSource.close();
        executor.shutdown();

        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdownNow();
        }

    }
}