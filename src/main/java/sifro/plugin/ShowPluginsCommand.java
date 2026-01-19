package sifro.plugin;

import com.hypixel.hytale.common.plugin.PluginIdentifier;
import com.hypixel.hytale.protocol.GameMode;
import com.hypixel.hytale.server.core.Message;
import com.hypixel.hytale.server.core.command.system.CommandContext;
import com.hypixel.hytale.server.core.command.system.arguments.system.DefaultArg;
import com.hypixel.hytale.server.core.command.system.arguments.system.OptionalArg;
import com.hypixel.hytale.server.core.command.system.arguments.system.RequiredArg;
import com.hypixel.hytale.server.core.command.system.arguments.types.ArgTypes;
import com.hypixel.hytale.server.core.command.system.arguments.types.BooleanFlagArgumentType;
import com.hypixel.hytale.server.core.command.system.basecommands.CommandBase;
import com.hypixel.hytale.server.core.plugin.PluginBase;
import com.hypixel.hytale.server.core.plugin.PluginManager;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShowPluginsCommand extends CommandBase {
    private final DefaultArg<Boolean> showHytalePluginsArg;

    public ShowPluginsCommand(String pluginName, String pluginVersion) {
        super("plugins", "Shows available plugins.");
        this.setPermissionGroup(GameMode.Creative);

        showHytalePluginsArg = withDefaultArg(
                "all",
                "Show all plugins, including Hytale's own",
                ArgTypes.BOOLEAN,
                false,
                "Disabled by default to reduce spam"
        );
    }

    @Override
    protected void executeSync(@Nonnull CommandContext ctx) {
        List<PluginBase> plugins = PluginManager.get().getPlugins();
        Map<String, ArrayList<String>> groups = new HashMap<>();

        for (PluginBase plugin: plugins) {
            String group = plugin.getManifest().getGroup();
            if (!showHytalePluginsArg.get(ctx) && group.startsWith("Hytale")) continue;

            String name = plugin.getManifest().getName();
            String version = plugin.getManifest().getVersion().toString();
            String text = name + " v" + version;

            if(!groups.containsKey(group)) {
                groups.put(group, new ArrayList<String>());
            }
            groups.get(group).add(text);
        };

        String str = "Plugins (" + plugins.size() + ")";

        for(String group: groups.keySet()) {
            str += "\n=====Group: " + group + "=====";
            for(String pluginInfo: groups.get(group)) {
                str += "\n" + pluginInfo;
            }
        }
        str += "\n====================";

        ctx.sendMessage(Message.raw(str));
    }
}
