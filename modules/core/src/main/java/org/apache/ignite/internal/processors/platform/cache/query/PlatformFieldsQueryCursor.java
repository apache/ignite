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

package org.apache.ignite.internal.processors.platform.cache.query;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;

import java.util.List;

/**
 * Interop cursor for fields query.
 */
public class PlatformFieldsQueryCursor extends PlatformAbstractQueryCursor<List<?>> {
    /** Gets field names. */
    private static final int OP_GET_FIELD_NAMES = 7;

    /**
     * Constructor.
     *
     * @param platformCtx Platform context.
     * @param cursor Backing cursor.
     * @param batchSize Batch size.
     */
    public PlatformFieldsQueryCursor(PlatformContext platformCtx, QueryCursorEx<List<?>> cursor, int batchSize) {
        super(platformCtx, cursor, batchSize);
    }

    /** {@inheritDoc} */
    @Override protected void write(BinaryRawWriterEx writer, List vals) {
        assert vals != null;

        int rowSizePos = writer.reserveInt();

        writer.writeInt(vals.size());

        for (Object val : vals)
            writer.writeObjectDetached(val);

        int rowEndPos = writer.out().position();
        
        writer.writeInt(rowSizePos, rowEndPos - rowSizePos);
    }

    /** {@inheritDoc} */
    @Override public void processOutStream(int type, final BinaryRawWriterEx writer) throws IgniteCheckedException {
        if (type == OP_GET_FIELD_NAMES) {
            FieldsQueryCursor fq = (FieldsQueryCursor) cursor();

            int cnt = fq.getColumnsCount();
            writer.writeInt(cnt);

            for (int i = 0; i < cnt; i++) {
                writer.writeString(fq.getFieldName(i));
            }
        } else {
            super.processOutStream(type, writer);
        }
    }
}
