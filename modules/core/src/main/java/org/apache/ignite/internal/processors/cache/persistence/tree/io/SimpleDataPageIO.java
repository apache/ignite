/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageDataRow;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Data pages IO for Metastorage.
 */
public class SimpleDataPageIO extends AbstractDataPageIO<MetastorageDataRow> {
    /** */
    public static final IOVersions<SimpleDataPageIO> VERSIONS = new IOVersions<>(
        new SimpleDataPageIO(1)
    );

    /**
     * @param ver Page format version.
     */
    public SimpleDataPageIO(int ver) {
        super(T_DATA_METASTORAGE, ver);
    }

    /** {@inheritDoc} */
    @Override
    protected void writeFragmentData(
        final MetastorageDataRow row,
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
    private int writeSizeFragment(final MetastorageDataRow row, final ByteBuffer buf, final int rowOff,
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
    @Override
    protected void writeRowData(
        long pageAddr,
        int dataOff,
        int payloadSize,
        MetastorageDataRow row,
        boolean newRow
    ) throws IgniteCheckedException {
        long addr = pageAddr + dataOff;

        if (newRow)
            PageUtils.putShort(addr, 0, (short)payloadSize);

        PageUtils.putInt(addr, 2, row.value().length);
        PageUtils.putBytes(addr, 6, row.value());
    }

    public static byte[] readPayload(long link) {
        int size = PageUtils.getInt(link, 0);

        return PageUtils.getBytes(link, 4, size);
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("SimpleDataPageIO [\n");
        printPageLayout(addr, pageSize, sb);
        sb.a("\n]");
    }
}
