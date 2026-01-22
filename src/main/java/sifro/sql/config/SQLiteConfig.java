package sifro.sql.config;

import java.nio.file.Path;

/**
 * Configuration class for SQLite database connections.
 */
public class SQLiteConfig extends DatabaseConfig{
    private final Path path;

    /**
     * Constructs a SQLiteConfig for the specified path.
     *
     * @param path     The path to the SQLite database.
     */
    public SQLiteConfig(String name, Path path) {
        super(name);
        this.path = path;
    }

    public Path getPath() { return path; }
}