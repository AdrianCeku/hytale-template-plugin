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
     * Single-thread executor for writes (Due to SQLite connections essentially being file handles, it is effectively single-writer).
     */
    private final ExecutorService writer;

    /**
     * Single-thread executor for reads.
     */
    private final ExecutorService reader;

    /**
     * Long-lived writer connection used by the write executor.
     */
    private final Connection writerConn;

    /**
     * Long-lived reader connection used by the read executor.
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
        if (conf.getPath().getParentFile() != null && !conf.getPath().getParentFile().exists()) {
            conf.getPath().getParentFile().mkdirs();
        }

        this.jdbcUrl = "jdbc:sqlite:" + conf.getPath().getAbsolutePath();

        this.writer = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, jdbcUrl + "-writer");
            t.setDaemon(true);
            return t;
        });

        this.reader = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, jdbcUrl + "reader");
            t.setDaemon(true);
            return t;
        });

        try {
            this.writerConn = DriverManager.getConnection(jdbcUrl);

            this.readerConn = DriverManager.getConnection(jdbcUrl);
            readerConn.setReadOnly(true);

            Statement wst = writerConn.createStatement();
            Statement rst = readerConn.createStatement();

            //Transaction journaling in a separate file.(https://www.sqlite.org/pragma.html#pragma_journal_mode)
            wst.execute("PRAGMA journal_mode=WAL;");

            // Better performance while still preventing data loss or corruption on system crashes, but pending executions might roll back (https://www.sqlite.org/pragma.html#pragma_synchronous)
            wst.execute("PRAGMA synchronous=NORMAL;");
            rst.execute("PRAGMA synchronous=NORMAL;");//Probably doesn't matter for read-only connection, but just in case.

            // 0x10002 = SQLITE_ANALYZE | SQLITE_OPTIMIZE (https://www.sqlite.org/pragma.html#pragma_optimize)
            wst.execute("PRAGMA optimize=0x10002;");



        } catch (SQLException e) {
            try { writer.shutdownNow(); } catch (Throwable ignored) {}
            try { reader.shutdownNow(); } catch (Throwable ignored) {}
            throw new RuntimeException("Failed to initialize SQLite connections", e);
        }

        this.optimizeScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, jdbcUrl + "-optimize");
            t.setDaemon(true);
            return t;
        });

        this.optimizeScheduler.scheduleAtFixedRate(() -> {
            try {
                Statement st = writerConn.createStatement();
                st.execute("PRAGMA optimize;");
            } catch (Throwable ignored) {
                System.err.println("Failed to run \"PRAGMA optimize\" on " + jdbcUrl);
            }
        }, 3, 3, TimeUnit.HOURS);
    }

    /**
     * Returns a JDBC connection to be used for a specific operation.
     *
     * <p>Writes use the dedicated writer connection; reads use the dedicated reader connection.</p>
     *
     * @param write returns writer connection if true, reader connection if false
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
        optimizeScheduler.shutdown();
        try {
            optimizeScheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } finally {
            optimizeScheduler.shutdownNow();
        }

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

        try { writerConn.close(); } catch (SQLException ignored) {}
        try { readerConn.close(); } catch (SQLException ignored) {}
    }
}