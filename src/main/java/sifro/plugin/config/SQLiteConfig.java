package sifro.plugin.config;

import java.io.File;

public class SQLiteConfig extends DBConfig {
    File path;

    public SQLiteConfig(File path, int poolSize) {
        super(poolSize);
        this.path = path;
    }

    public File getPath() {
        return path;
    }
}