package sifro.plugin.managers;

import sifro.plugin.config.SQLiteConfig;

import java.sql.*;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * SQLite manager implementation.
 *
 * <h2>Threading:</h2>
 * <ul>
 *   <li><b>Writes:</b> single dedicated writer thread using a long-lived connection.</li>
 *   <li><b>Reads:</b> single dedicated reader thread using a long-lived connection.</li>
 * </ul>
 *
 * <h2>PRAGMAs</h2>
 * On open:
 * <ul>
 *   <li>{@code PRAGMA journal_mode=WAL;}</li>
 *   <li>{@code PRAGMA synchronous=NORMAL;}</li>
 * </ul>
 *
 * On startup:
 * <ul>
 *   <li>{@code PRAGMA optimize=0x10002;}</li>
 * </ul>
 *
 * Periodically (every 3 hours):
 * <ul>
 *   <li>{@code PRAGMA optimize;}</li>
 * </ul>
 *
 * <p><b>Security warning:</b> {@link #executeAsync(String)} is dangerous if used with untrusted input.
 * Prefer prepared statement IDs via {@link #prepare(String)}.</p>
 *
 * </p><br>For more info see {@link DatabaseManager}.</p></p>
 */
public final class SQLiteDatabaseManager extends DatabaseManager {

    /**
     * JDBC URL for SQLite, e.g. {@code jdbc:sqlite:/path/to/db.sqlite}.
     */
    private final String jdbcUrl;

    /**
     * Single-thread executor used to serialize writes (SQLite is effectively single-writer).
     */
    private final ExecutorService writer;

    /**
     * Single-thread executor used to serialize reads (simple, predictable behavior).
     */
    private final ExecutorService reader;

    /**
     * Long-lived writer connection used by write executor tasks.
     */
    private final Connection writerConn;

    /**
     * Long-lived reader connection used by read executor tasks.
     */
    private final Connection readerConn;

    /**
     * Periodic scheduler for running {@code PRAGMA optimize;}.
     */
    private final ScheduledExecutorService optimizeScheduler;

    /**
     * Creates a SQLiteDatabaseManager.
     *
     * @param conf sqlite config containing path (and other settings)
     */
    public SQLiteDatabaseManager(SQLiteConfig conf) {
        super(null);
        Objects.requireNonNull(conf, "conf");

        // Ensure directory exists
        if (conf.getPath().getParentFile() != null && !conf.getPath().getParentFile().exists()) {
            //noinspection ResultOfMethodCallIgnored
            conf.getPath().getParentFile().mkdirs();
        }

        this.jdbcUrl = "jdbc:sqlite:" + conf.getPath().getAbsolutePath();

        // Writer executor: always one thread
        this.writer = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "sql-sqlite-writer");
            t.setDaemon(true);
            return t;
        });

        // Reader executor: one thread for simplicity
        this.reader = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "sql-sqlite-reader");
            t.setDaemon(true);
            return t;
        });

        // Open long-lived connections and apply PRAGMAs
        try {
            this.writerConn = DriverManager.getConnection(jdbcUrl);
            applyConnectionPragmas(writerConn, true);

            this.readerConn = DriverManager.getConnection(jdbcUrl);
            applyConnectionPragmas(readerConn, false);

            // Startup optimize flags (recommended)
            // 0x10002 = SQLITE_ANALYZE | SQLITE_OPTIMIZE
            runStatement(writerConn, "PRAGMA optimize=0x10002;");

        } catch (SQLException e) {
            // Clean up partial init best-effort
            try { writer.shutdownNow(); } catch (Throwable ignored) {}
            try { reader.shutdownNow(); } catch (Throwable ignored) {}
            throw new RuntimeException("Failed to initialize SQLite connections", e);
        }

        // Periodic optimize (every 3 hours by default)
        this.optimizeScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sql-sqlite-optimize");
            t.setDaemon(true);
            return t;
        });

        this.optimizeScheduler.scheduleAtFixedRate(() -> {
            // Run on writer connection to avoid concurrency and ensure it applies cleanly.
            try {
                runStatement(writerConn, "PRAGMA optimize;");
            } catch (Throwable ignored) {
            }
        }, 3, 3, TimeUnit.HOURS);
    }

    /**
     * Applies performance-oriented PRAGMAs to a SQLite connection.
     *
     * @param conn connection to configure
     * @param writerConn whether this connection will be used for writes
     * @throws SQLException if PRAGMA execution fails
     */
    private static void applyConnectionPragmas(Connection conn, boolean writerConn) throws SQLException {
        // WAL improves read concurrency and is generally recommended for multi-threaded access.
        runStatement(conn, "PRAGMA journal_mode=WAL;");

        // NORMAL is a common performance/durability compromise (fewer fsyncs than FULL).
        runStatement(conn, "PRAGMA synchronous=NORMAL;");

        // Optional: helps avoid immediate "database is locked" failures under contention.
        // Uncomment if you see "database is locked" frequently:
        // runStatement(conn, "PRAGMA busy_timeout=5000;");

        // Hint for reader connection (may be a no-op depending on driver)
        if (!writerConn) {
            try {
                conn.setReadOnly(true);
            } catch (SQLException ignored) {
            }
        }
    }

    /**
     * Executes a simple statement (used for PRAGMAs).
     *
     * @param conn target connection
     * @param sql sql statement to execute
     * @throws SQLException if execution fails
     */
    private static void runStatement(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /**
     * Provides a connection for a database operation.
     *
     * <p>Writes always use the dedicated writer connection; reads use the dedicated reader connection.</p>
     *
     * @param write true for write operations
     * @return connection to use
     */
    @Override
    protected Connection getConnection(boolean write) {
        return write ? writerConn : readerConn;
    }

    /**
     * @return executor for writes (single-threaded)
     */
    @Override
    protected ExecutorService writeExecutor() {
        return writer;
    }

    /**
     * @return executor for reads (single-threaded)
     */
    @Override
    protected ExecutorService readExecutor() {
        return reader;
    }

    /**
     * Closes the SQLite manager and releases all resources.
     *
     * <p>Shutdown sequence:
     * <ol>
     *   <li>stop optimize scheduler</li>
     *   <li>shutdown executors (best-effort wait)</li>
     *   <li>close connections</li>
     * </ol>
     * </p>
     */
    @Override
    public void close() {
        // Stop periodic optimize first
        optimizeScheduler.shutdown();
        try {
            optimizeScheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } finally {
            optimizeScheduler.shutdownNow();
        }

        // Stop accepting new DB work and let in-flight tasks finish
        writer.shutdown();
        reader.shutdown();
        try {
            writer.awaitTermination(5, TimeUnit.SECONDS);
            reader.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } finally {
            writer.shutdownNow();
            reader.shutdownNow();
        }

        // Close connections
        try { writerConn.close(); } catch (SQLException ignored) {}
        try { readerConn.close(); } catch (SQLException ignored) {}
    }
}