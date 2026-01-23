package io.github.adrianceku.hytale_sql_plugin.managers;

import io.github.adrianceku.hytale_sql_plugin.config.SQLiteConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
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
        Path dbPath = conf.getPath().toAbsolutePath().normalize();
        this.jdbcUrl = "jdbc:sqlite:" + dbPath;

        Path parentDir = dbPath.getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            try {
                Files.createDirectories(parentDir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create database directory: " + parentDir, e);
            }
        }

        System.out.println("[SQLite] Opening database at: " + dbPath);

        this.writer = Executors.newSingleThreadExecutor(r -> new Thread(r, jdbcUrl + "-writer"));
        this.reader = Executors.newSingleThreadExecutor(r -> new Thread(r, jdbcUrl + "-reader"));
        this.optimizeScheduler = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, jdbcUrl + "-optimize"));

        try {
            this.writerConn = DriverManager.getConnection(jdbcUrl);

            org.sqlite.SQLiteConfig readerCfg = new org.sqlite.SQLiteConfig();
            readerCfg.setReadOnly(true);
            this.readerConn = readerCfg.createConnection(jdbcUrl);

            try (Statement wst = writerConn.createStatement();
                 Statement rst = readerConn.createStatement()) {

                wst.execute("PRAGMA busy_timeout=5000;");
                rst.execute("PRAGMA busy_timeout=5000;");

                wst.execute("PRAGMA foreign_keys=ON;");
                rst.execute("PRAGMA foreign_keys=ON;");

                // Transaction journaling in a separate file. (https://www.sqlite.org/pragma.html#pragma_journal_mode)
                wst.execute("PRAGMA journal_mode=WAL;");
                rst.execute("PRAGMA journal_mode=WAL;"); // Probably doesn't matter for read-only connection, but just in case.

                // Better performance while still preventing data loss or corruption on system crashes,
                // but pending executions might roll back. (https://www.sqlite.org/pragma.html#pragma_synchronous)
                wst.execute("PRAGMA synchronous=NORMAL;");
                rst.execute("PRAGMA synchronous=NORMAL;"); // Probably doesn't matter for read-only connection, but just in case.

                // 0x10002 = SQLITE_ANALYZE | SQLITE_OPTIMIZE (https://www.sqlite.org/pragma.html#pragma_optimize)
                wst.execute("PRAGMA optimize=0x10002;");
            }

        } catch (SQLException e) {
            try {
                writer.shutdownNow();
            } catch (Throwable ignored) {
            }
            try {
                reader.shutdownNow();
            } catch (Throwable ignored) {
            }
            throw new RuntimeException("Failed to initialize SQLite connections", e);
        }

        // Optimize every  4 hours
        this.optimizeScheduler.scheduleAtFixedRate(() -> {
            try (Statement st = writerConn.createStatement()) {
                st.execute("PRAGMA optimize;");
            } catch (Throwable ignored) {
                System.err.println("Failed to run \"PRAGMA optimize\" on " + jdbcUrl);
            }
        }, 4, 4, TimeUnit.HOURS);
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
     * Releases a connection after a DB operation.
     *
     * <p>SQLite connections in this manager are long-lived and must NOT be closed per operation,
     * so this is intentionally passes.</p>
     *
     * @param connection connection used by the operation
     */
    @Override
    protected void closeConnection(Connection connection) {
        // pass
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
     *   <li>close connections</li>
     *   <li>shutdown executors (max. 5 sec wait)</li>
     * </ol>
     * </p>
     */
    @Override
    public void close() {
        System.out.println("\n\n===========\nCLOSING SQLITE DB MANAGER FOR " + jdbcUrl + "\n===========\n\n");
        optimizeScheduler.shutdown();
        writer.shutdown();
        reader.shutdown();

        try {
            writerConn.close();
            readerConn.close();

            optimizeScheduler.awaitTermination(5, TimeUnit.SECONDS);
            writer.awaitTermination(5, TimeUnit.SECONDS);
            reader.awaitTermination(5, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (SQLException e) {
            System.err.println("Failed to close SQLite connections for " + jdbcUrl);
        } finally {
            optimizeScheduler.shutdownNow();
            writer.shutdownNow();
            reader.shutdownNow();
        }
    }
}
