package sifro.plugin;

import com.hypixel.hytale.server.core.plugin.JavaPlugin;
import com.hypixel.hytale.server.core.plugin.JavaPluginInit;

import javax.annotation.Nonnull;


/**
 * Hytale plugin wrapper that exposes database functionality to other plugins.
 * Manages a default database connection and allows creating additional connections.
 */
public class SQL extends JavaPlugin {
    public SQL(@Nonnull JavaPluginInit init) {
        super(init);
    }

    @Override
    protected void setup() {
        this.getCommandRegistry().registerCommand(new QueryCommand(this.getName(), this.getManifest().getVersion().toString()));
    }
}
