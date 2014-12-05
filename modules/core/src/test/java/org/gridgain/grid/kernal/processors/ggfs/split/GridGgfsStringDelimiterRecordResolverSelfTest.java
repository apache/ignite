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

import java.nio.charset.*;

/**
 * String delimiter split resolver self-test.
 */
public class GridGgfsStringDelimiterRecordResolverSelfTest extends GridGgfsAbstractRecordResolverSelfTest {
    /** Charset used in tests. */
    private static final Charset UTF8 = Charset.forName("UTF-8");

    /**
     * Test split resolver.
     *
     * @throws Exception If failed.
     */
    public void testResolver() throws Exception {
        String delim = "aaaaaaaa";

        byte[] delimBytes = delim.getBytes(UTF8);
        byte[] data = array(F.t(wrap(1), 8), F.t(delimBytes, 1), F.t(wrap(1), 8));

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
        String... delims) throws Exception {
        write(data);

        GridGgfsByteDelimiterRecordResolver rslvr = resolver(delims);

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
    public void assertSplitNull(long suggestedStart, long suggestedLen, byte[] data, String... delims)
        throws Exception {
        write(data);

        GridGgfsStringDelimiterRecordResolver rslvr = resolver(delims);

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
    private GridGgfsStringDelimiterRecordResolver resolver(String... delims) {
        return new GridGgfsStringDelimiterRecordResolver(UTF8, delims);
    }
}
