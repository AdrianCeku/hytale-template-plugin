package sifro.plugin;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import sifro.plugin.config.DBConfig;
import sifro.plugin.config.MySQLConfig;
import sifro.plugin.config.SQLiteConfig;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a database connection pool.
 * Each instance manages its own HikariCP connection pool and prepared statements.
 */
public class DB {
    private HikariDataSource dataSource;
    private final ArrayList<String> preparedStatements = new ArrayList<>();
    private final AtomicInteger idCounter = new AtomicInteger(0);

    /**
     * Creates a new database connection using the provided configuration.
     *
     * @param conf       The database configuration
     */
    public DB(DBConfig conf) {
        HikariConfig hikariConfig = new HikariConfig();
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

        hikariConfig.setMaximumPoolSize(conf.getPoolSize());

        this.dataSource = new HikariDataSource(hikariConfig);
    }

    public void execute(String query) throws SQLException {
        Connection conn = dataSource.getConnection();
        conn.createStatement().execute(query);
        conn.close();
    }

}
