/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs.mapreduce.records;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.mapreduce.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Record resolver which adjusts records to fixed length. That is, start offset of the record is shifted to the
 * nearest position so that {@code newStart % length == 0}.
 */
public class GridGgfsFixedLengthRecordResolver implements GridGgfsRecordResolver, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Record length. */
    private long recLen;

    /**
     * Empty constructor required for {@link Externalizable} support.
     */
    public GridGgfsFixedLengthRecordResolver() {
        // No-op.
    }

    /**
     * Creates fixed-length record resolver.
     *
     * @param recLen Record length.
     */
    public GridGgfsFixedLengthRecordResolver(long recLen) {
        this.recLen = recLen;
    }

    /** {@inheritDoc} */
    @Override public GridGgfsFileRange resolveRecords(GridGgfs ggfs, GridGgfsInputStream stream,
        GridGgfsFileRange suggestedRecord)
        throws GridException, IOException {
        long suggestedEnd = suggestedRecord.start() + suggestedRecord.length();

        long startRem = suggestedRecord.start() % recLen;
        long endRem = suggestedEnd % recLen;

        long start = Math.min(suggestedRecord.start() + (startRem != 0 ? (recLen - startRem) : 0),
            stream.length());
        long end = Math.min(suggestedEnd + (endRem != 0 ? (recLen - endRem) : 0), stream.length());

        assert end >= start;

        return start != end ? new GridGgfsFileRange(suggestedRecord.path(), start, end - start) : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsFixedLengthRecordResolver.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(recLen);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        recLen = in.readLong();
    }
}
