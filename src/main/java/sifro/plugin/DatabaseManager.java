package sifro.plugin;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import sifro.plugin.config.DatabaseConfig;
import sifro.plugin.config.MySQLConfig;
import sifro.plugin.config.SQLiteConfig;


import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a database connection pool.
 * Each instance manages its own HikariCP connection pool and prepared statements.
 */
public class DatabaseManager {
    private final HikariDataSource dataSource;
    private final Map<Integer, String> statements = new ConcurrentHashMap<>();
    private final AtomicInteger idCounter = new AtomicInteger(0);
    private final ExecutorService executor;

    private final boolean enableExecute;

    /**
     * Creates a new database connection using the provided configuration.
     *
     * @param conf       The database configuration. See {@link MySQLConfig} and {@link SQLiteConfig} for more details.
     */
    public DatabaseManager(DatabaseConfig conf) {
        HikariConfig hikariConfig = new HikariConfig();
        this.enableExecute = conf.isEnableExecute();

        if(conf.getClass() == SQLiteConfig.class) {
            SQLiteConfig sqLiteConfig = (SQLiteConfig) conf;
            if(!sqLiteConfig.getPath().getParentFile().exists()) {
                sqLiteConfig.getPath().getParentFile().mkdirs();
            }
            String jdbcUrl = "jdbc:sqlite:" + sqLiteConfig.getPath().getAbsolutePath();
            hikariConfig.setJdbcUrl(jdbcUrl);

        } else if(conf.getClass() == MySQLConfig.class) {
            MySQLConfig mySQLConfig = (MySQLConfig) conf;

            String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s", mySQLConfig.getHost(), mySQLConfig.getPort(), mySQLConfig.getDatabase());
            hikariConfig.setJdbcUrl(jdbcUrl);

            hikariConfig.setUsername(mySQLConfig.getUsername());
            hikariConfig.setPassword(mySQLConfig.getPassword());
        }

        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        hikariConfig.setMaximumPoolSize(conf.getPoolSize());

        this.dataSource = new HikariDataSource(hikariConfig);

        this.executor = Executors.newFixedThreadPool(conf.getPoolSize());
    }

    /**
     * Executes a given SQL query. This method is disabled by default to prevent SQL Injection.
     * Enable it by calling DatabaseConfig.iUnderstandThisIsDangerous() on your configuration before passing it to the DatabaseManager.
     *
     * @param query The SQL query to execute.
     * @throws Exception if the execute method is disabled or if a database access error occurs.
     */
    public void execute(String query) throws SQLException {
        if(!this.enableExecute) throw new SQLException("Execute method is disabled. Use DatabaseConfig.iUnderstandThisIsDangerous() to enable it.");
        Connection conn = dataSource.getConnection();
        conn.createStatement().execute(query);
        conn.close();
    }

    public int prepare(String query) {
        int id = idCounter.incrementAndGet();
        statements.put(id, query);
        return id;
    }

    private void bindParameters(PreparedStatement stmt, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            stmt.setObject(i + 1, params[i]);
        }
    }

    /**
     * Executes an UPDATE/INSERT/DELETE statement asynchronously.
     *
     * @param id     The prepared statement ID
     * @param params The parameters to bind
     * @return CompletableFuture with affected row count
     */
    public CompletableFuture<Integer> update(int id, Object... params) {
        String query = statements.get(id);

        if (query == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("No statement with id: " + id)
            );
        }

        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(query)) {
                bindParameters(stmt, params);

                return stmt.executeUpdate();
            } catch (SQLException e) {
                throw new CompletionException(e);
            }
        }, this.executor);
    }

    /**
     * Executes a SELECT statement asynchronously.
     *
     * @param id     The prepared statement ID
     * @param params The parameters to bind
     * @return CompletableFuture with results as List of Maps
     */
    public CompletableFuture<List<Map<String, Object>>> query(int id, Object... params) {
        String query = statements.get(id);

        if (query == null) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("No statement with id: " + id)
            );
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                Connection conn = dataSource.getConnection();

                PreparedStatement stmt = conn.prepareStatement(query);
                bindParameters(stmt, params);
                ResultSet queryResult = stmt.executeQuery();

                return resultSetToList(queryResult);
            } catch (SQLException e) {
                throw new CompletionException(e);
            }
        }, executor);
    }

    private List<Map<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                row.put(meta.getColumnName(i), rs.getObject(i));
            }
            results.add(row);
        }
        return results;
    }

    public void close() {
        executor.shutdown();
        statements.clear();
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
