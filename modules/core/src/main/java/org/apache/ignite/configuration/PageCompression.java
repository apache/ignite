package org.apache.ignite.configuration;

/**
 * Page compression options.
 *
 * @see DataRegionConfiguration#setPageCompression
 * @see DataRegionConfiguration#setPageCompressionLevel
 */
public enum PageCompression {
    /** Only retain useful data from half-filled pages, but do not apply any compression. */
    DROP_GARBAGE,

    /** Zstd compression. */
    ZSTD,
}
