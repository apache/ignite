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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Data pages IO for writing binary arrays.
 */
public class SimpleDataPageIO extends AbstractDataPageIO<SimpleDataRow> {
    /** */
    public static final IOVersions<SimpleDataPageIO> VERSIONS = new IOVersions<>(
        new SimpleDataPageIO(1)
    );

    /**
     * @param ver Page format version.
     */
    public SimpleDataPageIO(int ver) {
        super(T_DATA_PART, ver);
    }

    /**
     * Constructor is intended for extending types.
     * @param type IO type.
     * @param ver Page format version.
     */
    public SimpleDataPageIO(int type, int ver) {
        super(type, ver);
    }

    /** {@inheritDoc} */
    @Override protected void writeFragmentData(
        final SimpleDataRow row,
        final ByteBuffer buf,
        final int rowOff,
        final int payloadSize
    ) throws IgniteCheckedException {
        int written = writeSizeFragment(row, buf, rowOff, payloadSize);

        if (payloadSize == written)
            return;

        int start = rowOff > 4 ? rowOff - 4 : 0;

        final int len = Math.min(row.value().length - start, payloadSize - written);

        if (len > 0) {
            buf.put(row.value(), start, len);
            written += len;
        }

        assert written == payloadSize;
    }

    /** */
    private int writeSizeFragment(final SimpleDataRow row, final ByteBuffer buf, final int rowOff,
        final int payloadSize) {
        final int size = 4;

        if (rowOff >= size)
            return 0;

        if (rowOff == 0 && payloadSize >= size) {
            buf.putInt(row.value().length);

            return size;
        }

        ByteBuffer buf2 = ByteBuffer.allocate(size);
        buf2.order(buf.order());

        buf2.putInt(row.value().length);
        int len = Math.min(size - rowOff, payloadSize);
        buf.put(buf2.array(), rowOff, len);

        return len;
    }

    /** {@inheritDoc} */
    @Override protected void writeRowData(
        long pageAddr,
        int dataOff,
        int payloadSize,
        SimpleDataRow row,
        boolean newRow
    ) throws IgniteCheckedException {
        long addr = pageAddr + dataOff;

        if (newRow)
            PageUtils.putShort(addr, 0, (short)payloadSize);

        PageUtils.putInt(addr, 2, row.value().length);
        PageUtils.putBytes(addr, 6, row.value());
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("SimpleDataPageIO [\n");
        printPageLayout(addr, pageSize, sb);
        sb.a("\n]");
    }
}
