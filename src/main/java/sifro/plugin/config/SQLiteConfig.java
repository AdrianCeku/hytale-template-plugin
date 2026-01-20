package sifro.plugin.config;

import java.io.File;

/**
 * Configuration class for SQLite database connections.
 */
public class SQLiteConfig extends DatabaseConfig {
    File path;

    /**
     * Constructs a SQLiteConfig with the specified parameters.
     *
     * @param path     The file path to the SQLite database.
     * @param poolSize The size of the connection pool.
     */
    public SQLiteConfig(File path, int poolSize) {
        super(poolSize);
        this.path = path;
    }

    public File getPath() {
        return path;
    }
}