package org.apache.ignite.internal.processors.odbc.protocol;

import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Supported commands.
 */
public enum GridOdbcCommand {
    /*
     * API commands.
     * =============
     */

    /** Execute sql query. */
    EXECUTE_SQL_QUERY("qryexe");

    /** Enum values. */
    private static final GridOdbcCommand[] VALS = values();

    /** Key to enum map. */
    private static final Map<String, GridOdbcCommand> cmds = new HashMap<>();

    /**
     * Map keys to commands.
     */
    static {
        for (GridOdbcCommand cmd : values())
            cmds.put(cmd.key(), cmd);
    }

    /** Command key. */
    private final String key;

    /**
     * @param key Key.
     */
    GridOdbcCommand(String key) {
        this.key = key;
    }

    /**
     * @param ord Byte to convert to enum.
     * @return Enum.
     */
    @Nullable
    public static GridOdbcCommand fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /**
     * @param key Key.
     * @return Command.
     */
    @Nullable public static GridOdbcCommand fromKey(String key) {
        return cmds.get(key);
    }

    /**
     * @return Command key.
     */
    public String key() {
        return key;
    }
}
