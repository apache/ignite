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

import static org.gridgain.grid.ggfs.mapreduce.records.IgniteFsNewLineRecordResolver.*;

/**
 * New line split resolver self test.
 */
public class GridGgfsNewLineDelimiterRecordResolverSelfTest extends GridGgfsAbstractRecordResolverSelfTest {
    /**
     * Test new line delimtier record resovler.
     *
     * @throws Exception If failed.
     */
    public void test() throws Exception{
        byte[] data = array(F.t(wrap(1), 8), F.t(wrap(SYM_LF), 1), F.t(wrap(1), 8), F.t(wrap(SYM_CR, SYM_LF), 1),
            F.t(wrap(1), 8));

        assertSplit(0, 4, 0, 9, data);
        assertSplit(0, 9, 0, 9, data);
        assertSplit(0, 13, 0, 19, data);
        assertSplit(0, 19, 0, 19, data);
        assertSplit(0, 23, 0, 27, data);
        assertSplit(0, 27, 0, 27, data);

        assertSplitNull(2, 2, data);
        assertSplitNull(2, 7, data);
        assertSplit(2, 11, 9, 10, data);
        assertSplit(2, 17, 9, 10, data);
        assertSplit(2, 21, 9, 18, data);
        assertSplit(2, 25, 9, 18, data);

        assertSplit(9, 4, 9, 10, data);
        assertSplit(9, 10, 9, 10, data);
        assertSplit(9, 14, 9, 18, data);
        assertSplit(9, 18, 9, 18, data);

        assertSplitNull(11, 2, data);
        assertSplitNull(11, 8, data);
        assertSplit(11, 12, 19, 8, data);
        assertSplit(11, 16, 19, 8, data);

        assertSplit(19, 4, 19, 8, data);
        assertSplit(19, 8, 19, 8, data);

        assertSplitNull(21, 2, data);
        assertSplitNull(21, 6, data);
    }

    /**
     * Check split resolution.
     *
     * @param suggestedStart Suggested start.
     * @param suggestedLen Suggested length.
     * @param expStart Expected start.
     * @param expLen Expected length.
     * @param data File data.
     * @throws Exception If failed.
     */
    public void assertSplit(long suggestedStart, long suggestedLen, long expStart, long expLen, byte[] data)
        throws Exception {
        write(data);

        IgniteFsNewLineRecordResolver rslvr = resolver();

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
     * @throws Exception If failed.
     */
    public void assertSplitNull(long suggestedStart, long suggestedLen, byte[] data)
        throws Exception {
        write(data);

        IgniteFsNewLineRecordResolver rslvr = resolver();

        IgniteFsFileRange split;

        try (IgniteFsInputStream is = read()) {
            split = rslvr.resolveRecords(ggfs, is, split(suggestedStart, suggestedLen));
        }

        assert split == null : "Split is not null.";
    }

    /**
     * Create resolver.
     *
     * @return Resolver.
     */
    private IgniteFsNewLineRecordResolver resolver() {
        return IgniteFsNewLineRecordResolver.NEW_LINE;
    }
}
