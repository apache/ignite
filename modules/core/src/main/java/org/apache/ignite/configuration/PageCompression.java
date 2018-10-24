package org.apache.ignite.configuration;

/**
 * Page compression options.
 */
public enum PageCompression {
    /** Only retain useful data from pages, but do not apply any compression. */
    DROP_GARBAGE(0),

    /** Zstd with better speed and lower compression ratio. */
    ZSTD_BETTER_SPEED(1),

    /** Zstd with default compression and speed. */
    ZSTD_DEFAULT(3),

    /** Zstd with better compression ratio and lower speed.*/
    ZSTD_BETTER_COMPRESSION(19);

    /** */
    private final int level;

    /**
     * @param level Compression level.
     */
    PageCompression(int level) {
        this.level = level;
    }

    /**
     * Gets compression level.
     *
     * @return Compression level.
     */
    public int getLevel() {
        return level;
    }
}
