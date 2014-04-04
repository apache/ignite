/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs.split;

import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.mapreduce.*;
import org.gridgain.grid.ggfs.mapreduce.records.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Fixed length split resolver self test.
 */
public class GridGgfsFixedLengthRecordResolverSelfTest extends GridGgfsAbstractRecordResolverSelfTest {
    /**
     * Test split resolver.
     *
     * @throws Exception If failed.
     */
    public void testResolver() throws Exception {
        byte[] data = array(F.t(wrap(1), 24));

        assertSplit(0, 4, 0, 8, data, 8);
        assertSplit(0, 8, 0, 8, data, 8);
        assertSplit(0, 12, 0, 16, data, 8);
        assertSplit(0, 16, 0, 16, data, 8);
        assertSplit(0, 20, 0, 24, data, 8);
        assertSplit(0, 24, 0, 24, data, 8);
        assertSplit(0, 28, 0, 24, data, 8);
        assertSplit(0, 32, 0, 24, data, 8);

        assertSplitNull(2, 2, data, 8);
        assertSplitNull(2, 6, data, 8);
        assertSplit(2, 10, 8, 8, data, 8);
        assertSplit(2, 14, 8, 8, data, 8);
        assertSplit(2, 18, 8, 16, data, 8);
        assertSplit(2, 22, 8, 16, data, 8);
        assertSplit(2, 26, 8, 16, data, 8);
        assertSplit(2, 30, 8, 16, data, 8);

        assertSplit(8, 4, 8, 8, data, 8);
        assertSplit(8, 8, 8, 8, data, 8);
        assertSplit(8, 12, 8, 16, data, 8);
        assertSplit(8, 16, 8, 16, data, 8);
        assertSplit(8, 20, 8, 16, data, 8);
        assertSplit(8, 24, 8, 16, data, 8);

        assertSplitNull(10, 2, data, 8);
        assertSplitNull(10, 6, data, 8);
        assertSplit(10, 10, 16, 8, data, 8);
        assertSplit(10, 14, 16, 8, data, 8);
        assertSplit(10, 18, 16, 8, data, 8);
        assertSplit(10, 22, 16, 8, data, 8);

        assertSplit(16, 4, 16, 8, data, 8);
        assertSplit(16, 8, 16, 8, data, 8);
        assertSplit(16, 12, 16, 8, data, 8);
        assertSplit(16, 16, 16, 8, data, 8);

        assertSplitNull(18, 2, data, 8);
        assertSplitNull(18, 6, data, 8);
        assertSplitNull(18, 10, data, 8);
        assertSplitNull(18, 14, data, 8);

        assertSplitNull(24, 4, data, 8);
        assertSplitNull(24, 8, data, 8);

        assertSplitNull(26, 2, data, 8);
        assertSplitNull(26, 6, data, 8);
    }

    /**
     * Check split resolution.
     *
     * @param suggestedStart Suggested start.
     * @param suggestedLen Suggested length.
     * @param expStart Expected start.
     * @param expLen Expected length.
     * @param data File data.
     * @param len Length.
     * @throws Exception If failed.
     */
    public void assertSplit(long suggestedStart, long suggestedLen, long expStart, long expLen, byte[] data, int len)
        throws Exception {
        write(data);

        GridGgfsFixedLengthRecordResolver rslvr = resolver(len);

        GridGgfsFileRange split;

        try (GridGgfsInputStream is = read()) {
            split = rslvr.resolveRecords(ggfs, is, split(suggestedStart, suggestedLen));
        }

        assert split != null : "Split is null.";
        assert split.start() == expStart : "Incorrect start [expected=" + expStart + ", actual=" + split.start() + ']';
        assert split.length() == expLen : "Incorrect length [expected=" + expLen + ", actual=" + split.length() + ']';
    }

    /**
     * Check the split resolution resulted in {@code null}.
     *
     * @param suggestedStart Suggested start.
     * @param suggestedLen Suggested length.
     * @param data File data.
     * @param len Length.
     * @throws Exception If failed.
     */
    public void assertSplitNull(long suggestedStart, long suggestedLen, byte[] data, int len)
        throws Exception {
        write(data);

        GridGgfsFixedLengthRecordResolver rslvr = resolver(len);

        GridGgfsFileRange split;

        try (GridGgfsInputStream is = read()) {
            split = rslvr.resolveRecords(ggfs, is, split(suggestedStart, suggestedLen));
        }

        assert split == null : "Split is not null.";
    }

    /**
     * Create resolver.
     *
     * @param len Length.
     * @return Resolver.
     */
    private GridGgfsFixedLengthRecordResolver resolver(int len) {
        return new GridGgfsFixedLengthRecordResolver(len);
    }
}
