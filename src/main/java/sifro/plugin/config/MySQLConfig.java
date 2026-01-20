package sifro.plugin.config;
/**
 * Configuration class for MySQL database connections.
 */
public class MySQLConfig extends DatabaseConfig {
    String host;
    String port;
    String database;
    String username;
    String password;

    /**
     * Constructs a MySQLConfig with the specified parameters.
     *
     * @param host     The hostname of the MySQL server.
     * @param port     The port number of the MySQL server.
     * @param database The name of the database.
     * @param username The username for authentication.
     * @param password The password for authentication.
     * @param poolSize The size of the connection pool.
     */
    MySQLConfig(String host, String port, String database, String username, String password, int poolSize) {
        super(poolSize);
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    public String getPassword() {
        return this.password;
    }

    public String getUsername() {
        return this.username;
    }

    public String getDatabase() {
        return this.database;
    }

    public String getPort() {
        return this.port;
    }

    public String getHost() {
        return this.host;
    }
}
