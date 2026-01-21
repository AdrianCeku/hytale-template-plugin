package sifro.plugin;

import com.hypixel.hytale.server.core.plugin.JavaPlugin;
import com.hypixel.hytale.server.core.plugin.JavaPluginInit;
import sifro.plugin.config.ConfigReader;
import sifro.plugin.config.DatabaseConfig;
import sifro.plugin.config.MySQLConfig;
import sifro.plugin.config.SQLiteConfig;
import sifro.plugin.managers.DatabaseManager;
import sifro.plugin.managers.MySQLDatabaseManager;
import sifro.plugin.managers.SQLiteDatabaseManager;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Hytale plugin that exposes database functionality to other plugins.
 * Manages database connections loaded from a JSON configuration file.
 *
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
 * MySQLConfig config = new MySQLConfig("localhost", "3306", "mydb", "user", "pass", 10);
 * MySQLDatabaseManager db = SQL.register("mydb", config);
 * }</pre>
 *
 * <p><b>Registering a SQLite database programmatically:</b>
 * <pre>{@code
 * SQLiteConfig config = new SQLiteConfig("analytics", Path.of("./analytics.db"));
 * SQLiteDatabaseManager db = SQL.register("analytics", config);
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
    private static final String CONFIG_FILE_NAME = "default_db.json";
    private static final Path CONFIG_PATH = Path.of("./" + CONFIG_FILE_NAME);

    private static DatabaseManager defaultDatabase;
    private static final Map<String, DatabaseManager> databases = new ConcurrentHashMap<>();

    static {
        initDefaultDatabase();
    }

    private static void initDefaultDatabase() {
        if (Files.exists(CONFIG_PATH)) {
            try {
                DatabaseConfig config = ConfigReader.readOne(CONFIG_PATH);
                defaultDatabase = createManager(config);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load default database config from " + CONFIG_FILE_NAME, e);
            }
        } else {
            defaultDatabase = new SQLiteDatabaseManager(new SQLiteConfig("default", Path.of("./data/database.db")));
        }
    }

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
     * <p>Example usage:
     * <pre>{@code
     * DatabaseManager db = SQL.getDatabase();
     * int queryId = db.prepare("SELECT * FROM players WHERE uuid = ?");
     * db.query(queryId, uuid).thenAccept(results -> {
     *     // process results
     * });
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
     * int insertId = analytics.prepare("INSERT INTO events (type, data) VALUES (?, ?)");
     * analytics.update(insertId, eventType, eventData);
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
     * Registers a new database connection programmatically.
     * Automatically determines the correct manager type based on the config.
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
