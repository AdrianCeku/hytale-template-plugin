package sifro.plugin.config;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a database configuration entry from the JSON config file.
 */
public class DatabaseConfigEntry {
    private String name;
    private String type; // "mysql" or "sqlite"
    private boolean enableExecute = false;
    private Integer poolSize; // nullable, uses default if not set

    // SQLite specific
    private String path;

    // MySQL specific
    private String host;
    private String port;
    private String database;
    private String user;
    private String password;

    /**
     * Converts this entry to the appropriate DatabaseConfig subclass.
     *
     * @param baseDir The base directory for resolving relative SQLite paths
     * @return The DatabaseConfig instance
     */
    public DatabaseConfig toConfig(File baseDir) {
        DatabaseConfig config;

        if ("mysql".equalsIgnoreCase(type)) {
            int pool = poolSize != null ? poolSize : 10;
            config = new MySQLConfig(host, port, database, user, password, pool);
        } else if ("sqlite".equalsIgnoreCase(type)) {
            int pool = poolSize != null ? poolSize : 3;
            File dbPath = new File(path);
            if (!dbPath.isAbsolute()) {
                dbPath = new File(baseDir, path);
            }
            config = new SQLiteConfig(dbPath, pool);
        } else {
            throw new IllegalArgumentException("Unknown database type: " + type);
        }

        if (enableExecute) {
            config.iUnderstandThisIsDangerous();
        }

        return config;
    }

    public String getName() {
        return name;
    }

    /**
     * Loads database configurations from a JSON file.
     *
     * @param configFile The JSON config file
     * @return List of database config entries
     * @throws IOException if the file cannot be read
     */
    public static List<DatabaseConfigEntry> loadFromFile(File configFile) throws IOException {
        if (!configFile.exists()) {
            createDefaultConfig(configFile);
        }


        Gson gson = new Gson();
        Type listType = new TypeToken<ArrayList<DatabaseConfigEntry>>() {}.getType();
        try (Reader reader = Files.newBufferedReader(Path.of(configFile.getAbsolutePath()))) {
            return gson.fromJson(reader, listType);
        }
    }

    /**
     * Creates a default configuration file with example entries.
     */
    private static void createDefaultConfig(File configFile) throws IOException {
        String defaultConfig = """
            [
                {
                    "name": "default",
                    "type": "sqlite",
                    "path": "database.db"
                }
            ]
            """;

        configFile.mkdir();
        try (Writer writer = new FileWriter(configFile)) {
            writer.write(defaultConfig);
        }
    }
}
