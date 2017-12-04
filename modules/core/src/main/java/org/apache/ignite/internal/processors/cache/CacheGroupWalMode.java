package org.apache.ignite.internal.processors.cache;

/**
 *
 */
public enum CacheGroupWalMode {
    /** Enabling. */
    ENABLING,

    /** Enable. */
    ENABLE,

    /** Disabling. */
    DISABLING,

    /** Disable. */
    DISABLE;

    /**
     * @param disable Disable.
     * @param prepare Prepare.
     */
    public static CacheGroupWalMode resolve(boolean disable, boolean prepare) {
        if (disable) {
            if (prepare)
                return DISABLING;
            else
                return DISABLE;
        }
        else {
            if (prepare)
                return ENABLING;
            else
                return ENABLE;
        }
    }
}
