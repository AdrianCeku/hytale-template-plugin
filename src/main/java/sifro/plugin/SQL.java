package sifro.plugin;

import com.hypixel.hytale.server.core.plugin.JavaPlugin;
import com.hypixel.hytale.server.core.plugin.JavaPluginInit;
import sifro.plugin.config.DatabaseConfig;
import sifro.plugin.config.DatabaseConfigEntry;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.List;
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
    private static final Map<String, DatabaseManager> databases = new ConcurrentHashMap<>();
    private static File baseDir;
    private static boolean initialized = false;

    public SQL(@Nonnull JavaPluginInit init) {
        super(init);
    }

    @Override
    protected void setup() {
        System.out.println("===========\n\n\n\nSQL Plugin Setup\n\n\n=========== ");
        baseDir = getDataDirectory().toFile();
        System.out.println(baseDir);
        File configFile = new File(baseDir, CONFIG_FILE_NAME);
        System.out.println(configFile.getAbsolutePath());
        try {
            List<DatabaseConfigEntry> entries = DatabaseConfigEntry.loadFromFile(configFile);

            for (DatabaseConfigEntry entry : entries) {
                DatabaseConfig config = entry.toConfig(baseDir);
                DatabaseManager manager = new DatabaseManager(config);
                databases.put(entry.getName(), manager);
            }

            initialized = true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize databases", e);
        }
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
        return getDatabase("default");
    }

    /**
     * Gets a named database connection.
     *
     * @param name The name of the database as defined in the config file
     * @return The DatabaseManager for the named database
     * @throws IllegalStateException    if SQL not initialized
     * @throws IllegalArgumentException if no database with that name exists
     */
    public static DatabaseManager getDatabase(String name) {
        if (!initialized) {
            throw new IllegalStateException("SQL has not been initialized yet.");
        }

        DatabaseManager manager = databases.get(name);
        if (manager == null) {
            throw new IllegalArgumentException("No database configured with name: " + name);
        }

        return manager;
    }

    /**
     * Checks if a database with the given name is configured.
     *
     * @param name The database name
     * @return true if the database exists
     */
    public static boolean hasDatabase(String name) {
        return databases.containsKey(name);
    }

    /**
     * Registers a new database connection programmatically.
     *
     * @param name   The name for this database
     * @param config The database configuration
     * @return The created DatabaseManager
     * @throws IllegalArgumentException if a database with that name already exists
     */
    public static DatabaseManager register(String name, DatabaseConfig config) {
        if (databases.containsKey(name)) {
            throw new IllegalArgumentException("Database with name '" + name + "' already exists");
        }

        DatabaseManager manager = new DatabaseManager(config);
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
        for (DatabaseManager manager : databases.values()) {
            manager.close();
        }
        databases.clear();
        initialized = false;
    }
}
