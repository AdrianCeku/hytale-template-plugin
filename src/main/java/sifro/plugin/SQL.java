package sifro.plugin;

import com.hypixel.hytale.server.core.plugin.JavaPlugin;
import com.hypixel.hytale.server.core.plugin.JavaPluginInit;
import sifro.plugin.config.DatabaseConfig;
import sifro.plugin.config.MySQLConfig;
import sifro.plugin.config.SQLiteConfig;
import sifro.plugin.managers.DatabaseManager;
import sifro.plugin.managers.MySQLDatabaseManager;
import sifro.plugin.managers.SQLiteDatabaseManager;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Hytale plugin that exposes database functionality to other plugins.
 * Manages database connections loaded from a JSON configuration file.
 *
 * <p>Configuration is loaded from {@code databases.json} in the plugin's data directory.
 *
 * <p>Example databases.json:
 * <pre>{@code
 * [
 *     {
 *         "name": "default",
 *         "type": "sqlite",
 *         "path": "database.db"
 *     },
 *     {
 *         "name": "analytics",
 *         "type": "mysql",
 *         "host": "localhost",
 *         "port": "3306",
 *         "database": "hytale",
 *         "user": "root",
 *         "password": "password",
 *         "enableExecute": true
 *     }
 * ]
 * }</pre>
 *
 * <p>Usage from other plugins:
 * <pre>{@code
 * // Get the default database
 * DatabaseManager db = SQL.getDatabase();
 *
 * // Get a named database
 * DatabaseManager analytics = SQL.getDatabase("analytics");
 *
 * // Prepare and execute queries
 * int insertId = db.prepare("INSERT INTO players (uuid, name) VALUES (?, ?)");
 * db.update(insertId, uuid, name).thenAccept(rows -> ...);
 * }</pre>
 */
public class SQL extends JavaPlugin {
    private static final String CONFIG_FILE_NAME = "databases.json";

    private static final DatabaseManager defaultDatabase;
    private static final Map<String, DatabaseManager> databases = new ConcurrentHashMap<>();

    public SQL(@Nonnull JavaPluginInit init) {
        super(init);
    }

    @Override
    protected void setup() {

    }

    /**
     * Closes all database connections on shutdown.
     */
    public void shutdown() {
        closeAll();
    }

    // ==================== Public API ====================

    /**
     * Gets the default database connection.
     * This is the database with name "default" in the config file.
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
     * @param name The name of the database as defined in the config file
     * @return The DatabaseManager for the named database, or null if not found
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
     * @param name The database name
     * @return true if the database exists, false otherwise
     */
    public static boolean hasDatabase(String name) {
        return databases.containsKey(name);
    }

    /**
     * Registers a new MySQL database connection programmatically.
     *
     * @param name   The name for this database
     * @param config The database configuration
     * @return The created DatabaseManager
     * @throws IllegalArgumentException if a database with that name already exists
     */
    public static MySQLDatabaseManager register(String name, MySQLConfig config) throws IllegalArgumentException{
        if (databases.containsKey(name)) {
            throw new IllegalArgumentException("Database with name '" + name + "' already exists");
        }

        MySQLDatabaseManager manager = new MySQLDatabaseManager(config);
        databases.put(name, manager);

        return manager;
    }

    /**
     * Registers a new SQLite database connection programmatically.
     *
     * @param name   The name for this database
     * @param config The database configuration
     * @return The created DatabaseManager
     * @throws IllegalArgumentException if a database with that name already exists
     */
    public static SQLiteDatabaseManager register(String name, SQLiteConfig config) throws IllegalArgumentException{
        if (databases.containsKey(name)) {
            throw new IllegalArgumentException("Database with name '" + name + "' already exists");
        }

        SQLiteDatabaseManager manager = new SQLiteDatabaseManager(config);
        databases.put(name, manager);

        return manager;
    }

    /**
     * Closes a specific database connection.
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
        ArrayList<DatabaseManager> managers = new ArrayList<>(databases.values());
        databases.clear();

        for (DatabaseManager manager : managers) {
            manager.close();
        }
    }
}
