/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs.split;

import org.apache.ignite.fs.*;
import org.gridgain.grid.ggfs.mapreduce.*;
import org.gridgain.grid.ggfs.mapreduce.records.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Byte delimiter split resolver self test.
 */
public class GridGgfsByteDelimiterRecordResolverSelfTest extends GridGgfsAbstractRecordResolverSelfTest {
    /**
     * Test split resolution when there are no delimiters in the file.
     *
     * @throws Exception If failed.
     */
    public void testNoDelimiters() throws Exception {
        byte[] delim = wrap(2);
        byte[] data = array(F.t(wrap(1), 8));

        assertSplit(0, 4, 0, 8, data, delim);
        assertSplit(0, 8, 0, 8, data, delim);

        assertSplitNull(2, 2, data, delim);
        assertSplitNull(2, 6, data, delim);
    }

    /**
     * Test split resolution when there is one delimiter at the head.
     *
     * @throws Exception If failed.
     */
    public void testHeadDelimiter() throws Exception {
        byte[] delim = array(F.t(wrap(2), 8));
        byte[] data = array(F.t(delim, 1), F.t(wrap(1), 8));

        assertSplit(0, 4, 0, 8, data, delim);
        assertSplit(0, 8, 0, 8, data, delim);
        assertSplit(0, 12, 0, 16, data, delim);
        assertSplit(0, 16, 0, 16, data, delim);

        assertSplitNull(2, 2, data, delim);
        assertSplitNull(2, 6, data, delim);
        assertSplit(2, 10, 8, 8, data, delim);
        assertSplit(2, 14, 8, 8, data, delim);

        assertSplit(8, 4, 8, 8, data, delim);
        assertSplit(8, 8, 8, 8, data, delim);

        assertSplitNull(10, 2, data, delim);
        assertSplitNull(10, 6, data, delim);
    }

    /**
     * Test split when there is one delimiter at the end.
     *
     * @throws Exception If failed.
     */
    public void testEndDelimiter() throws Exception {
        byte[] delim = array(F.t(wrap(2), 8));
        byte[] data = array(F.t(wrap(1), 8), F.t(delim, 1));

        assertSplit(0, 4, 0, 16, data, delim);
        assertSplit(0, 8, 0, 16, data, delim);
        assertSplit(0, 12, 0, 16, data, delim);
        assertSplit(0, 16, 0, 16, data, delim);

        assertSplitNull(2, 2, data, delim);
        assertSplitNull(2, 6, data, delim);
        assertSplitNull(2, 10, data, delim);
        assertSplitNull(2, 14, data, delim);

        assertSplitNull(8, 4, data, delim);
        assertSplitNull(8, 8, data, delim);

        assertSplitNull(10, 2, data, delim);
        assertSplitNull(10, 6, data, delim);
    }

    /**
     * Test split when there is one delimiter in the middle.
     *
     * @throws Exception If failed.
     */
    public void testMiddleDelimiter() throws Exception {
        byte[] delim = array(F.t(wrap(2), 8));
        byte[] data = array(F.t(wrap(1), 8), F.t(delim, 1), F.t(wrap(1), 8));

        assertSplit(0, 4, 0, 16, data, delim);
        assertSplit(0, 8, 0, 16, data, delim);
        assertSplit(0, 12, 0, 16, data, delim);
        assertSplit(0, 16, 0, 16, data, delim);
        assertSplit(0, 20, 0, 24, data, delim);
        assertSplit(0, 24, 0, 24, data, delim);

        assertSplitNull(2, 2, data, delim);
        assertSplitNull(2, 6, data, delim);
        assertSplitNull(2, 10, data, delim);
        assertSplitNull(2, 14, data, delim);
        assertSplit(2, 18, 16, 8, data, delim);
        assertSplit(2, 22, 16, 8, data, delim);

        assertSplitNull(8, 4, data, delim);
        assertSplitNull(8, 8, data, delim);
        assertSplit(8, 12, 16, 8, data, delim);
        assertSplit(8, 16, 16, 8, data, delim);

        assertSplitNull(10, 2, data, delim);
        assertSplitNull(10, 6, data, delim);
        assertSplit(10, 10, 16, 8, data, delim);
        assertSplit(10, 14, 16, 8, data, delim);

        assertSplit(16, 4, 16, 8, data, delim);
        assertSplit(16, 8, 16, 8, data, delim);

        assertSplitNull(18, 2, data, delim);
        assertSplitNull(18, 6, data, delim);
    }

    /**
     * Test split when there are two head delimiters.
     *
     * @throws Exception If failed.
     */
    public void testTwoHeadDelimiters() throws Exception {
        byte[] delim = array(F.t(wrap(2), 8));
        byte[] data = array(F.t(delim, 2), F.t(wrap(1), 8));

        assertSplit(0, 4, 0, 8, data, delim);
        assertSplit(0, 8, 0, 8, data, delim);
        assertSplit(0, 12, 0, 16, data, delim);
        assertSplit(0, 16, 0, 16, data, delim);
        assertSplit(0, 20, 0, 24, data, delim);
        assertSplit(0, 24, 0, 24, data, delim);

        assertSplitNull(2, 2, data, delim);
        assertSplitNull(2, 6, data, delim);
        assertSplit(2, 10, 8, 8, data, delim);
        assertSplit(2, 14, 8, 8, data, delim);
        assertSplit(2, 18, 8, 16, data, delim);
        assertSplit(2, 22, 8, 16, data, delim);

        assertSplit(8, 4, 8, 8, data, delim);
        assertSplit(8, 8, 8, 8, data, delim);
        assertSplit(8, 12, 8, 16, data, delim);
        assertSplit(8, 16, 8, 16, data, delim);

        assertSplitNull(10, 2, data, delim);
        assertSplitNull(10, 6, data, delim);
        assertSplit(10, 10, 16, 8, data, delim);
        assertSplit(10, 14, 16, 8, data, delim);

        assertSplit(16, 4, 16, 8, data, delim);
        assertSplit(16, 8, 16, 8, data, delim);

        assertSplitNull(18, 2, data, delim);
        assertSplitNull(18, 6, data, delim);
    }

    /**
     * Test split when there are two tail delimiters.
     *
     * @throws Exception If failed.
     */
    public void testTwoTailDelimiters() throws Exception {
        byte[] delim = array(F.t(wrap(2), 8));
        byte[] data = array(F.t(wrap(1), 8), F.t(delim, 2));

        assertSplit(0, 4, 0, 16, data, delim);
        assertSplit(0, 8, 0, 16, data, delim);
        assertSplit(0, 12, 0, 16, data, delim);
        assertSplit(0, 16, 0, 16, data, delim);
        assertSplit(0, 20, 0, 24, data, delim);
        assertSplit(0, 24, 0, 24, data, delim);

        assertSplitNull(2, 2, data, delim);
        assertSplitNull(2, 6, data, delim);
        assertSplitNull(2, 10, data, delim);
        assertSplitNull(2, 14, data, delim);
        assertSplit(2, 18, 16, 8, data, delim);
        assertSplit(2, 22, 16, 8, data, delim);

        assertSplitNull(8, 4, data, delim);
        assertSplitNull(8, 8, data, delim);
        assertSplit(8, 12, 16, 8, data, delim);
        assertSplit(8, 16, 16, 8, data, delim);

        assertSplitNull(10, 2, data, delim);
        assertSplitNull(10, 6, data, delim);
        assertSplit(10, 10, 16, 8, data, delim);
        assertSplit(10, 14, 16, 8, data, delim);

        assertSplit(16, 4, 16, 8, data, delim);
        assertSplit(16, 8, 16, 8, data, delim);

        assertSplitNull(18, 2, data, delim);
        assertSplitNull(18, 6, data, delim);
    }

    /**
     * Test split when there is one head delimiter, one tail delimiter and some data between them.
     *
     * @throws Exception If failed.
     */
    public void testHeadAndTailDelimiters() throws Exception {
        byte[] delim = array(F.t(wrap(2), 8));
        byte[] data = array(F.t(delim, 1), F.t(wrap(1), 8), F.t(delim, 1));

        assertSplit(0, 4, 0, 8, data, delim);
        assertSplit(0, 8, 0, 8, data, delim);
        assertSplit(0, 12, 0, 24, data, delim);
        assertSplit(0, 16, 0, 24, data, delim);
        assertSplit(0, 20, 0, 24, data, delim);
        assertSplit(0, 24, 0, 24, data, delim);

        assertSplitNull(2, 2, data, delim);
        assertSplitNull(2, 6, data, delim);
        assertSplit(2, 10, 8, 16, data, delim);
        assertSplit(2, 14, 8, 16, data, delim);
        assertSplit(2, 18, 8, 16, data, delim);
        assertSplit(2, 22, 8, 16, data, delim);

        assertSplit(8, 4, 8, 16, data, delim);
        assertSplit(8, 8, 8, 16, data, delim);
        assertSplit(8, 12, 8, 16, data, delim);
        assertSplit(8, 16, 8, 16, data, delim);

        assertSplitNull(10, 2, data, delim);
        assertSplitNull(10, 6, data, delim);
        assertSplitNull(10, 10, data, delim);
        assertSplitNull(10, 14, data, delim);

        assertSplitNull(16, 4, data, delim);
        assertSplitNull(16, 8, data, delim);

        assertSplitNull(18, 2, data, delim);
        assertSplitNull(18, 6, data, delim);
    }

    /**
     * Test special case when delimiter starts with the same bytes as the last previos data byte.
     *
     * @throws Exception If failed.
     */
    public void testDelimiterStartsWithTheSameBytesAsLastPreviousDataByte() throws Exception {
        byte[] delim = array(F.t(wrap(1, 1, 2), 1));
        byte[] data = array(F.t(wrap(1), 1), F.t(delim, 1), F.t(wrap(1), 1));

        assertSplit(0, 1, 0, 4, data, delim);
        assertSplit(0, 2, 0, 4, data, delim);
        assertSplit(0, 4, 0, 4, data, delim);
        assertSplit(0, 5, 0, 5, data, delim);

        assertSplit(1, 4, 4, 1, data, delim);
    }

    /**
     * Check split resolution.
     *
     * @param suggestedStart Suggested start.
     * @param suggestedLen Suggested length.
     * @param expStart Expected start.
     * @param expLen Expected length.
     * @param data File data.
     * @param delims Delimiters.
     * @throws Exception If failed.
     */
    public void assertSplit(long suggestedStart, long suggestedLen, long expStart, long expLen, byte[] data,
        byte[]... delims) throws Exception {
        write(data);

        IgniteFsByteDelimiterRecordResolver rslvr = resolver(delims);

        IgniteFsFileRange split;

        try (IgniteFsInputStream is = read()) {
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
     * @param delims Delimiters.
     * @throws Exception If failed.
     */
    public void assertSplitNull(long suggestedStart, long suggestedLen, byte[] data, byte[]... delims)
        throws Exception {
        write(data);

        IgniteFsByteDelimiterRecordResolver rslvr = resolver(delims);

        IgniteFsFileRange split;

        try (IgniteFsInputStream is = read()) {
            split = rslvr.resolveRecords(ggfs, is, split(suggestedStart, suggestedLen));
        }

        assert split == null : "Split is not null.";
    }

    /**
     * Create resolver.
     *
     * @param delims Delimiters.
     * @return Resolver.
     */
    private IgniteFsByteDelimiterRecordResolver resolver(byte[]... delims) {
        return new IgniteFsByteDelimiterRecordResolver(delims);
    }
}
