package sifro.plugin.config;

public abstract class DBConfig {
    private int poolSize;

    DBConfig(int poolSize) {
        this.poolSize = poolSize;
    }

    public int getPoolSize() {
        return poolSize;
    }
};

