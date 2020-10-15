package org.apache.ignite.internal.commandline.persistence;

import org.apache.ignite.internal.commandline.argument.CommandArg;

/**
 * {@link PersistenceSubcommands#CLEAN} subcommand arguments.
 */
public enum CleanAndBackupSubcommandArg implements CommandArg {
    /** Clean all caches data files. */
    ALL("all"),
    /** Clean corrupted caches data files. */
    CORRUPTED("corrupted"),
    /** Clean only specified caches data files. */
    CACHES("caches");

    /** */
    private final String name;

    /** */
    CleanAndBackupSubcommandArg(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return name;
    }
}
