package sifro.plugin.managers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import sifro.plugin.config.MySQLConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
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
     * Executor used for both reads and writes.
     */
    private final ExecutorService executor;

    /**
     * Creates a MySQLDatabaseManager using the provided config.
     *
     * @param conf MySQL configuration
     */
    public MySQLDatabaseManager(MySQLConfig conf) {
        super(createDataSource(conf));
        this.executor = Executors.newCachedThreadPool();
    }

    /**
     * Builds and configures a Hikari datasource for MySQL.
     *
     * @param conf mysql config
     * @return configured datasource
     */
    private static HikariDataSource createDataSource(MySQLConfig conf) {
        Objects.requireNonNull(conf, "conf");

        HikariConfig cfg = new HikariConfig();

        String jdbcUrl = String.format(
                "jdbc:mysql://%s:%s/%s",
                conf.getHost(),
                conf.getPort(),
                conf.getDatabase()
        );

        cfg.setJdbcUrl(jdbcUrl);
        cfg.setUsername(conf.getUsername());
        cfg.setPassword(conf.getPassword());

        // Pool sizing
        cfg.setMaximumPoolSize(Math.max(1, conf.getPoolSize()));
        cfg.setMinimumIdle(Math.min(2, Math.max(0, conf.getPoolSize())));

        // Timeouts
        cfg.setConnectionTimeout(5_000);
        cfg.setValidationTimeout(2_000);

        // Lifetime tuning
        cfg.setIdleTimeout(60_000);
        cfg.setMaxLifetime(30 * 60_000); // 30 minutes

        // We generally don't need keepalives unless you have NAT/proxy idle disconnects
        cfg.setKeepaliveTime(0);

        cfg.setAutoCommit(true);
        cfg.setPoolName("SQL-mysql-" + Integer.toHexString(System.identityHashCode(conf)));

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
     *   <li>shutdown executor (best-effort wait)</li>
     *   <li>close Hikari datasource</li>
     * </ol>
     * </p>
     */
    @Override
    public void close() {
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdownNow();
        }

        hikariDataSource.close();
    }
}