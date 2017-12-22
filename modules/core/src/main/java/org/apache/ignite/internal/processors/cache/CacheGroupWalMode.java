package org.apache.ignite.internal.processors.cache;

/**
 *
 */
public enum CacheGroupWalMode {
    /** Enable. */
    ENABLED,

    /** Enabling. */
    ENABLING,

    /** Disable. */
    DISABLED,

    /** Disabling. */
    DISABLING;

    /**
     * @param disable Disable.
     * @param prepare Prepare.
     */
    public static CacheGroupWalMode resolve(boolean disable, boolean prepare) {
        if (disable) {
            if (prepare)
                return DISABLING;
            else
                return DISABLED;
        }
        else {
            if (prepare)
                return ENABLING;
            else
                return ENABLED;
        }
    }
}
