package sifro.plugin.config;

import java.nio.file.Path;

/**
 * Configuration class for SQLite database connections.
 */
public class SQLiteConfig extends DatabaseConfig{
    Path path;

    /**
     * Constructs a SQLiteConfig for the specified path.
     *
     * @param path     The path to the SQLite database.
     */
    public SQLiteConfig(Path path) {
        this.path = path;
    }

    public Path getPath() {
        return path;
    }
}