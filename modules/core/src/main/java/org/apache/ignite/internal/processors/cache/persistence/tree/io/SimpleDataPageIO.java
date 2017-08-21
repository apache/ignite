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
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageDataRow;

/**
 * Data pages IO.
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
        final int len = Math.min(row.value().length - rowOff, payloadSize);

        buf.put(row.value(), rowOff, len);
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

        if (newRow) {
            PageUtils.putShort(addr, 0, (short)payloadSize);
            addr += 2;
        }
        else
            addr += 2;

        PageUtils.putBytes(addr, 0, row.value());
    }

    @Override public int getRowSize(MetastorageDataRow row) throws IgniteCheckedException {
        return 2 + row.value().length;
    }
}
