package sifro.plugin;

import com.hypixel.hytale.common.plugin.PluginIdentifier;
import com.hypixel.hytale.protocol.GameMode;
import com.hypixel.hytale.server.core.Message;
import com.hypixel.hytale.server.core.command.system.CommandContext;
import com.hypixel.hytale.server.core.command.system.arguments.system.RequiredArg;
import com.hypixel.hytale.server.core.command.system.arguments.types.ArgTypes;
import com.hypixel.hytale.server.core.command.system.basecommands.CommandBase;
import com.hypixel.hytale.server.core.plugin.PluginBase;
import com.hypixel.hytale.server.core.plugin.PluginManager;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class ReloadPluginCommand extends CommandBase {
    private final RequiredArg<String> groupNameArg;
    private final RequiredArg<String> pluginNameArg;

    public ReloadPluginCommand(String pluginName, String pluginVersion) {
        super("reload", "Reloads a given plugin.");
        this.setPermissionGroup(GameMode.Creative);
        groupNameArg = withRequiredArg("group", "The group of the plugin", ArgTypes.STRING);
        pluginNameArg = withRequiredArg("plugin", "The plugin to restart", ArgTypes.STRING);
    }

    @Override
    protected void executeSync(@Nonnull CommandContext ctx) {
        String group = groupNameArg.get(ctx);
        String plugin = pluginNameArg.get(ctx);
        String fullPluginName = group + ":" + plugin;

        List<PluginBase> plugins = PluginManager.get().getPlugins();

        PluginIdentifier pluginId = PluginIdentifier.fromString(fullPluginName);

        Boolean found = false;
        for(PluginBase p: plugins) {
            if(p.getIdentifier().equals(pluginId)) {
                found = true;
                break;
            }
        }

        if(!found) {
            ctx.sendMessage(Message.raw("Plugin \"" + fullPluginName + "\" not found!"));
            return;
        }

        ctx.sendMessage(Message.raw("Reloading plugin \"" + fullPluginName + "\" ..."));

        PluginManager.get().reload(pluginId);
    }
}
