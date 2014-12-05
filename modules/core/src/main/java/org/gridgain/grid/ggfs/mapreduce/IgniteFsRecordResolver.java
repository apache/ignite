/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs.mapreduce;

import org.apache.ignite.*;
import org.apache.ignite.fs.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * GGFS record resolver. When {@link IgniteFsTask} is split into {@link IgniteFsJob}s each produced job will obtain
 * {@link IgniteFsFileRange} based on file data location. Record resolver is invoked in each job before actual
 * execution in order to adjust record boundaries in a way consistent with user data.
 * <p>
 * E.g., you may want to split your task into jobs so that each job process zero, one or several lines from that file.
 * But file is split into ranges based on block locations, not new line boundaries. Using convenient record resolver
 * you can adjust job range so that it covers the whole line(s).
 * <p>
 * The following record resolvers are available out of the box:
 * <ul>
 *     <li>{@link org.gridgain.grid.ggfs.mapreduce.records.IgniteFsFixedLengthRecordResolver}</li>
 *     <li>{@link org.gridgain.grid.ggfs.mapreduce.records.IgniteFsByteDelimiterRecordResolver}</li>
 *     <li>{@link org.gridgain.grid.ggfs.mapreduce.records.IgniteFsStringDelimiterRecordResolver}</li>
 *     <li>{@link org.gridgain.grid.ggfs.mapreduce.records.IgniteFsNewLineRecordResolver}</li>
 * </ul>
 */
public interface IgniteFsRecordResolver extends Serializable {
    /**
     * Adjusts record start offset and length.
     *
     * @param ggfs GGFS instance to use.
     * @param stream Input stream for split file.
     * @param suggestedRecord Suggested file system record.
     * @return New adjusted record. If this method returns {@code null}, original record is ignored.
     * @throws GridException If resolve failed.
     * @throws IOException If resolve failed.
     */
    @Nullable public IgniteFsFileRange resolveRecords(IgniteFs ggfs, IgniteFsInputStream stream,
        IgniteFsFileRange suggestedRecord) throws GridException, IOException;
}
