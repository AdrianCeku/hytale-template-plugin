package sifro.plugin.config;

/**
 * Abstract class representing database configuration.
 */
public abstract class DatabaseConfig {
    private final int poolSize;
    private boolean enableExecute = false;

    DatabaseConfig(int poolSize) {
        this.poolSize = poolSize;
    }

    public int getPoolSize() {
        return this.poolSize;
    }

    /**
     * !!!Enables the execute method. This can lead to SQL Injection if used improperly.!!!
     */
    public void iUnderstandThisIsDangerous() {
        this.enableExecute = true;
    }

    public boolean isEnableExecute() {
        return this.enableExecute;
    }
}

