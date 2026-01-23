package io.github.adrianceku.hytale_sql_plugin.config;
/**
 * Configuration class for MySQL database connections, stores basic auth and connection info.
 */
public class MySQLConfig extends DatabaseConfig {
    private final String host;
    private final String port;
    private final String database;
    private final String username;
    private final String password;
    private final int poolSize;

    /**
     * Constructs a MySQLConfig with the specified parameters.
     *
     * @param host     The hostname of the MySQL server.
     * @param port     The port number of the MySQL server.
     * @param database The name of the database.
     * @param username The username for authentication.
     * @param password The password for authentication.
     * @param poolSize The size of the HikariCP pool.
     */
    MySQLConfig(String name, String host, String port, String database, String username, String password, int poolSize) {
        super(name);
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
        this.poolSize = poolSize;
    }

    public String getPassword() { return this.password; }

    public String getUsername() { return this.username; }

    public String getDatabase() { return this.database; }

    public String getPort() { return this.port; }

    public String getHost() { return this.host; }

    public int getPoolSize() { return this.poolSize; }
}
