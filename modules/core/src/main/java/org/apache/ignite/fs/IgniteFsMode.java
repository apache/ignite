/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.fs;

import org.jetbrains.annotations.*;

/**
 * {@code GGFS} mode defining interactions with underlying secondary Hadoop file system.
 * Secondary Hadoop file system is provided for pass-through, write-through, and
 * read-through purposes.
 * <p>
 * This mode is configured via {@link IgniteFsConfiguration#getDefaultMode()}
 * configuration property.
 */
public enum IgniteFsMode {
    /**
     * In this mode GGFS will not delegate to secondary Hadoop file system and will
     * cache all the files in memory only.
     */
    PRIMARY,

    /**
     * In this mode GGFS will not cache any files in memory and will only pass them
     * through to secondary Hadoop file system. If this mode is enabled, then
     * secondary Hadoop file system must be configured.
     *
     * @see IgniteFsConfiguration#getSecondaryHadoopFileSystemUri()
     */
    PROXY,

    /**
     * In this mode {@code GGFS} will cache files locally and also <i>synchronously</i>
     * write them through to secondary Hadoop file system.
     * <p>
     * If secondary Hadoop file system is not configured, then this mode behaves like
     * {@link #PRIMARY} mode.
     *
     * @see IgniteFsConfiguration#getSecondaryHadoopFileSystemUri()
     */
    DUAL_SYNC,

    /**
     * In this mode {@code GGFS} will cache files locally and also <i>asynchronously</i>
     * write them through to secondary Hadoop file system.
     * <p>
     * If secondary Hadoop file system is not configured, then this mode behaves like
     * {@link #PRIMARY} mode.
     *
     * @see IgniteFsConfiguration#getSecondaryHadoopFileSystemUri()
     */
    DUAL_ASYNC;

    /** Enumerated values. */
    private static final IgniteFsMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static IgniteFsMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
