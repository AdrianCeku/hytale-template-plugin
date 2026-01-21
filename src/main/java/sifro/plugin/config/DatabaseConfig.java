package sifro.plugin.config;

import javax.annotation.Nonnull;

/**
 * Base configuration class for database connections.
 *
 * Contains just a name and serves as a parent for specific database configurations.
 */
public abstract class DatabaseConfig {
    private final String name;

    DatabaseConfig(String name) {
        this.name = name;
    }

    public String getName() { return name; }
}
