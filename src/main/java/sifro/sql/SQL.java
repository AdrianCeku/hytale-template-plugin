package sifro.sql;

import com.hypixel.hytale.server.core.plugin.JavaPlugin;
import com.hypixel.hytale.server.core.plugin.JavaPluginInit;
import sifro.sql.config.ConfigReader;
import sifro.sql.config.DatabaseConfig;
import sifro.sql.config.MySQLConfig;
import sifro.sql.config.SQLiteConfig;
import sifro.sql.managers.DatabaseManager;
import sifro.sql.managers.MySQLDatabaseManager;
import sifro.sql.managers.SQLiteDatabaseManager;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Hytale plugin that exposes database functionality to other plugins.
 * Manages database connections loaded from a JSON configuration file.
 * Prefer using the default database using {@link #getDatabase()} connection unless multiple databases are required.
 * <p>Configuration is loaded from {@code default_db.json} in the working directory.
 *
 * <p>Example default_db.json (SQLite):
 * <pre>{@code
 * {
 *     "name": "default",
 *     "type": "sqlite",
 *     "path": "database.db"
 * }
 * }</pre>
 *
 * <p>Example default_db.json (MySQL):
 * <pre>{@code
 * {
 *     "name": "default",
 *     "type": "mysql",
 *     "host": "localhost",
 *     "port": "3306",
 *     "database": "hytale",
 *     "user": "root",
 *     "password": "password",
 *     "poolSize": 10
 * }
 * }</pre>
 *
 * <h2>Usage Examples</h2>
 *
 * <p><b>Getting the default database:</b>
 * <pre>{@code
 * DatabaseManager db = SQL.getDatabase();
 * }</pre>
 *
 * <p><b>Getting a named database:</b>
 * <pre>{@code
 * DatabaseManager analytics = SQL.getNamedDatabase("analytics");
 * }</pre>
 *
 * <p><b>Checking if a database exists:</b>
 * <pre>{@code
 * if (SQL.hasDatabase("analytics")) {
 *     DatabaseManager analytics = SQL.getNamedDatabase("analytics");
 * }
 * }</pre>
 *
 * <p><b>Registering a MySQL database programmatically:</b>
 * <pre>{@code
 * MySQLConfig config = new MySQLConfig("mydbname","localhost", "3306", "mydb", "user", "pass", 10);
 * MySQLDatabaseManager db = SQL.register(config);
 * }</pre>
 *
 * <p><b>Registering a SQLite database programmatically:</b>
 * <pre>{@code
 * SQLiteConfig config = new SQLiteConfig("analytics", Path.of("./analytics.db"));
 * SQLiteDatabaseManager db = SQL.register(config);
 * }</pre>
 *
 * <p><b>Registering a database using generic config:</b>
 * <pre>{@code
 * DatabaseConfig config = ConfigReader.readOne(Path.of("./custom_db.json"));
 * DatabaseManager db = SQL.register(config);
 * }</pre>
 *
 * <p><b>Closing a specific database:</b>
 * <pre>{@code
 * SQL.close("analytics");
 * }</pre>
 *
 * <p><b>Preparing and executing queries:</b>
 * <pre>{@code
 * DatabaseManager db = SQL.getDatabase();
 * int insertId = db.prepare("INSERT INTO players (uuid, name) VALUES (?, ?)");
 * db.update(insertId, uuid, name).thenAccept(rows -> {
 *     System.out.println("Inserted " + rows + " row(s)");
 * });
 * }</pre>
 */
public class SQL extends JavaPlugin {
    private static final DatabaseManager defaultDatabase = createManager(ConfigReader.readDefault());
    private static final Map<String, DatabaseManager> databases = new ConcurrentHashMap<>();


    public SQL(@Nonnull JavaPluginInit init) {
        super(init);
    }

    @Override
    protected void setup() {
        System.out.println("\n\n\n\n==============\n[SQL Plugin] Initializing database connections...\n==============\n\n\n\n");
        this.getCommandRegistry().registerCommand(new QueryCommand(this.getName(), this.getManifest().getVersion().toString()));
        System.out.println("Registered /query command.");
    }

    /**
     * Closes all database connections on shutdown.
     */
    @Override
    public void shutdown() {
        closeAll();
    }

    // ==================== Public API ====================

    /**
     * Gets the default database connection.
     * This is the database with name "default" in the config file.
     * Prefer using this database and avoid creating multiple databases unless necessary.
     * <p>Example usage:
     * <pre>{@code
     * DatabaseManager db = SQL.getDatabase();
     * }</pre>
     *
     * @return The default DatabaseManager
     * @throws IllegalStateException if no default database is configured or SQL not initialized
     */
    public static DatabaseManager getDatabase() {
        return defaultDatabase;
    }

    /**
     * Gets a named database connection.
     *
     * <p>Example usage:
     * <pre>{@code
     * DatabaseManager analytics = SQL.getNamedDatabase("analytics");
     * }</pre>
     *
     * @param name The name of the database as defined in the config file
     * @return The DatabaseManager for the named database, or null if not found
     * @throws IllegalArgumentException if no database with the given name exists
     */
    public static DatabaseManager getNamedDatabase(String name) throws IllegalArgumentException {
        if(!hasDatabase(name)) {
            throw new IllegalArgumentException("No database with name '" + name + "' exists");
        }

        return databases.get(name);
    }

    /**
     * Checks if a database with the given name is configured.
     *
     * <p>Example usage:
     * <pre>{@code
     * if (SQL.hasDatabase("analytics")) {
     *     DatabaseManager analytics = SQL.getNamedDatabase("analytics");
     *     // use analytics database
     * } else {
     *     // fallback or register new database
     * }
     * }</pre>
     *
     * @param name The database name
     * @return true if the database exists, false otherwise
     */
    public static boolean hasDatabase(String name) {
        return databases.containsKey(name);
    }

    /**
     * Registers a new database connection programmatically.
     * Automatically determines the correct manager type based on the config.
     *
     * <p>Example usage:
     * <pre>{@code
     * DatabaseConfig config = ConfigReader.readOne(Path.of("./custom_db.json"));
     * DatabaseManager db = SQL.register(config);
     * }</pre>
     *
     * @param config The database configuration (MySQLConfig or SQLiteConfig)
     * @return The created DatabaseManager
     * @throws IllegalArgumentException if a database with that name already exists or unknown config type
     */
    public static DatabaseManager register( DatabaseConfig config) throws IllegalArgumentException {
        if (databases.containsKey(config.getName())) {
            throw new IllegalArgumentException("Database with name '" + config.getName() + "' already exists");
        }

        DatabaseManager manager = createManager(config);
        databases.put(config.getName(), manager);

        return manager;
    }

    /**
     * Creates a DatabaseManager from a DatabaseConfig.
     *
     * @param config The database configuration
     * @return The appropriate DatabaseManager for the config type
     */
    private static DatabaseManager createManager(DatabaseConfig config) {
        return switch (config) {
            case MySQLConfig mysql -> new MySQLDatabaseManager(mysql);
            case SQLiteConfig sqlite -> new SQLiteDatabaseManager(sqlite);
            default -> throw new IllegalArgumentException("Unknown database config type: " + config.getClass().getName());
        };
    }

    /**
     * Closes a specific database connection.
     *
     * <p>Example usage:
     * <pre>{@code
     * // Close a database when no longer needed
     * SQL.close("analytics");
     *
     * // Can also be used in cleanup logic
     * if (SQL.hasDatabase("temp")) {
     *     SQL.close("temp");
     * }
     * }</pre>
     *
     * @param name The name of the database to close
     */
    public static void close(String name) {
        DatabaseManager manager = databases.remove(name);
        if (manager != null) {
            manager.close();
        }
    }

    /**
     * Closes all database connections.
     */
    private static void closeAll() {
        System.out.println("\n\n===========\nClosing all database connections...\n\n===========\n");
        ArrayList<DatabaseManager> managers = new ArrayList<>(databases.values());
        databases.clear();

        for (DatabaseManager manager : managers) {
            manager.close();
        }
    }
}
