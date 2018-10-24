package org.apache.ignite.configuration;

/**
 * Page compression options.
 *
 * @see DataRegionConfiguration#setPageCompression(PageCompression)
 * @see DataRegionConfiguration#setPageCompressionLevel(int)
 */
public enum PageCompression {
    /** Only retain useful data from pages, but do not apply any compression. */
    DROP_GARBAGE,

    /** Zstd compression. */
    ZSTD,
}
