package sifro.plugin.config;

public class MySQLConfig extends DBConfig {
    String host;
    String port;
    String database;
    String username;
    String password;

    MySQLConfig(String host, String port, String database, String username, String password, int poolSize) {
        super(poolSize);
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    public String getUsername() {
        return username;
    }

    public String getDatabase() {
        return database;
    }

    public String getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }
}
