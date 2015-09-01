/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.igfs.split;

import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.mapreduce.IgfsFileRange;
import org.apache.ignite.igfs.mapreduce.records.IgfsNewLineRecordResolver;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.igfs.mapreduce.records.IgfsNewLineRecordResolver.SYM_CR;
import static org.apache.ignite.igfs.mapreduce.records.IgfsNewLineRecordResolver.SYM_LF;

/**
 * New line split resolver self test.
 */
public class IgfsNewLineDelimiterRecordResolverSelfTest extends IgfsAbstractRecordResolverSelfTest {
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

        IgfsNewLineRecordResolver rslvr = resolver();

        IgfsFileRange split;

        try (IgfsInputStream is = read()) {
            split = rslvr.resolveRecords(igfs, is, split(suggestedStart, suggestedLen));
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

        IgfsNewLineRecordResolver rslvr = resolver();

        IgfsFileRange split;

        try (IgfsInputStream is = read()) {
            split = rslvr.resolveRecords(igfs, is, split(suggestedStart, suggestedLen));
        }

        assert split == null : "Split is not null.";
    }

    /**
     * Create resolver.
     *
     * @return Resolver.
     */
    private IgfsNewLineRecordResolver resolver() {
        return IgfsNewLineRecordResolver.NEW_LINE;
    }
}