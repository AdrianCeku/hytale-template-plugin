package sifro.plugin.config;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads database configurations from JSON files.
 */
public class ConfigReader {
    private static final Gson GSON = new Gson();
    private static final Path DEFAULT_CONFIG_PATH = Path.of("./default_db.json");
    private static final String DEFAULT_CONFIG_CONTENT = """
            {
                "name": "default",
                "type": "sqlite",
                "path": "./data/database.db"
            }
            """;

    /**
     * Reads the default database configuration from {@code ./default_db.json}.
     * If the file does not exist, creates it with a default SQLite configuration.
     *
     * @return DatabaseConfig parsed from the default config file
     * @throws RuntimeException if the file cannot be read or created
     */
    public static DatabaseConfig readDefault() {
        try {
            if (!Files.exists(DEFAULT_CONFIG_PATH)) {
                Files.writeString(DEFAULT_CONFIG_PATH, DEFAULT_CONFIG_CONTENT);
            }
            return readOne(DEFAULT_CONFIG_PATH);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read or create default database config", e);
        }
    }

    /**
     * Reads a single database configuration from a JSON file.
     *
     * @param path Path to the JSON configuration file
     * @return DatabaseConfig parsed from the file
     * @throws IOException if the file cannot be read
     */
    public static DatabaseConfig readOne(Path path) throws IOException {
        String content = Files.readString(path);
        return parseOne(content);
    }

    /**
     * Reads multiple database configurations from a JSON array file.
     *
     * @param path Path to the JSON configuration file
     * @return List of DatabaseConfig parsed from the file
     * @throws IOException if the file cannot be read
     */
    public static List<DatabaseConfig> readMany(Path path) throws IOException {
        String content = Files.readString(path);
        return parseMany(content);
    }

    /**
     * Parses a single database configuration from a JSON string.
     *
     * @param json JSON string containing a single database configuration
     * @return DatabaseConfig parsed from the JSON
     */
    public static DatabaseConfig parseOne(String json) {
        JsonObject obj = GSON.fromJson(json, JsonObject.class);
        return parseConfig(obj);
    }

    /**
     * Parses multiple database configurations from a JSON array string.
     *
     * @param json JSON string containing an array of database configurations
     * @return List of DatabaseConfig parsed from the JSON
     */
    public static List<DatabaseConfig> parseMany(String json) {
        List<DatabaseConfig> configs = new ArrayList<>();
        JsonArray array = GSON.fromJson(json, JsonArray.class);

        for (JsonElement element : array) {
            configs.add(parseConfig(element.getAsJsonObject()));
        }

        return configs;
    }

    private static DatabaseConfig parseConfig(JsonObject obj) {
        String type = getOptional(obj, "type", "sqlite", String.class);

        return switch (type.toLowerCase()) {
            case "mysql" -> new MySQLConfig(
                    getRequired(obj, "name", String.class),
                    getRequired(obj, "host", String.class),
                    getOptional(obj, "port", "3306", String.class),
                    getRequired(obj, "database", String.class),
                    getRequired(obj, "user", String.class),
                    getRequired(obj, "password", String.class),
                    getOptional(obj, "poolSize", 10, Integer.class)
            );
            case "sqlite" -> new SQLiteConfig(
                    getRequired(obj, "name", String.class),
                    Path.of(getRequired(obj, "path", String.class))
            );
            default -> throw new IllegalArgumentException("Unknown database type: " + type);
        };
    }

    private static <T> T getRequired(JsonObject obj, String key, Class<T> type) {
        if (!obj.has(key)) {
            throw new IllegalArgumentException("Missing required field: " + key);
        }
        return getValue(obj, key, type);
    }

    private static <T> T getOptional(JsonObject obj, String key, T defaultValue, Class<T> type) {
        return obj.has(key) ? getValue(obj, key, type) : defaultValue;
    }

    @SuppressWarnings("unchecked")
    private static <T> T getValue(JsonObject obj, String key, Class<T> type) {
        if (type == String.class) {
            return (T) obj.get(key).getAsString();
        } else if (type == Integer.class) {
            return (T) Integer.valueOf(obj.get(key).getAsInt());
        } else if (type == Boolean.class) {
            return (T) Boolean.valueOf(obj.get(key).getAsBoolean());
        }
        throw new IllegalArgumentException("Unsupported type: " + type.getName());
    }
}
