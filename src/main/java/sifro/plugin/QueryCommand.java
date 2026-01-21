package sifro.plugin;

import com.hypixel.hytale.protocol.GameMode;
import com.hypixel.hytale.server.core.Message;
import com.hypixel.hytale.server.core.command.system.CommandContext;
import com.hypixel.hytale.server.core.command.system.arguments.system.RequiredArg;
import com.hypixel.hytale.server.core.command.system.arguments.types.ArgTypes;
import com.hypixel.hytale.server.core.command.system.basecommands.CommandBase;
import sifro.plugin.config.SQLiteConfig;
import sifro.plugin.managers.DatabaseManager;

import javax.annotation.Nonnull;
import java.io.File;

/**
 * This is an example command that will simply print the name of the plugin in chat when used.
 */
public class QueryCommand extends CommandBase {
    private final RequiredArg<String> query;
    private final DatabaseManager databaseManager;

    public QueryCommand(String pluginName, String pluginVersion) {
        super("query", "Executes a query on the db.");
        this.setPermissionGroup(GameMode.Creative); // Allows the command to be used by anyone, not just OP
        query = withRequiredArg("query", "The SQL query to execute.", ArgTypes.STRING);

        SQLiteConfig conf = new SQLiteConfig(new File("./test.sqlite"), 10);
        databaseManager = new DatabaseManager(conf);
    }

    @Override
    protected void executeSync(@Nonnull CommandContext ctx) {
        String sqlQuery = query.get(ctx);
        if (sqlQuery.startsWith("\"") && sqlQuery.endsWith("\"")) {
            sqlQuery = sqlQuery.substring(1, sqlQuery.length() - 1);
        }
        try {
            databaseManager.execute(sqlQuery);
        } catch (Exception e) {
            ctx.sendMessage(Message.raw(e.getMessage()));
        }
    }
}