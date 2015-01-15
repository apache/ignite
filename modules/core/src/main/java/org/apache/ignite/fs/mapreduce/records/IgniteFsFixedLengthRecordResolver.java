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

package org.apache.ignite.fs.mapreduce.records;

import org.apache.ignite.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.fs.mapreduce.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Record resolver which adjusts records to fixed length. That is, start offset of the record is shifted to the
 * nearest position so that {@code newStart % length == 0}.
 */
public class IgniteFsFixedLengthRecordResolver implements IgniteFsRecordResolver, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Record length. */
    private long recLen;

    /**
     * Empty constructor required for {@link Externalizable} support.
     */
    public IgniteFsFixedLengthRecordResolver() {
        // No-op.
    }

    /**
     * Creates fixed-length record resolver.
     *
     * @param recLen Record length.
     */
    public IgniteFsFixedLengthRecordResolver(long recLen) {
        this.recLen = recLen;
    }

    /** {@inheritDoc} */
    @Override public IgniteFsFileRange resolveRecords(IgniteFs ggfs, IgniteFsInputStream stream,
        IgniteFsFileRange suggestedRecord)
        throws IgniteCheckedException, IOException {
        long suggestedEnd = suggestedRecord.start() + suggestedRecord.length();

        long startRem = suggestedRecord.start() % recLen;
        long endRem = suggestedEnd % recLen;

        long start = Math.min(suggestedRecord.start() + (startRem != 0 ? (recLen - startRem) : 0),
            stream.length());
        long end = Math.min(suggestedEnd + (endRem != 0 ? (recLen - endRem) : 0), stream.length());

        assert end >= start;

        return start != end ? new IgniteFsFileRange(suggestedRecord.path(), start, end - start) : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteFsFixedLengthRecordResolver.class, this);
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
