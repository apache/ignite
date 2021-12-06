package org.apache.ignite.internal.commandline.cache.argument;

import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.cache.CacheMetrics;

/**
 * Command arguments for {@link CacheMetrics} command.
 */
public enum CacheMetricsCommandArg implements CommandArg {
    /** Enable. */
    ENABLE("--enable"),

    /** Disable. */
    DISABLE("--disable"),

    /** Status. */
    STATUS("--status"),

    /** Perform command for all caches instead of defined list. */
    ALL_CACHES("--all-caches");

    /** Enable statistics flag. */
    private final String name;

    /** */
    CacheMetricsCommandArg(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
