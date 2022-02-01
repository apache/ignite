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

package org.apache.ignite.internal.processors.platform.cache.query;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;

/**
 * Interop cursor for fields query.
 */
@SuppressWarnings("rawtypes")
public class PlatformFieldsQueryCursor extends PlatformAbstractQueryCursor<List<?>> {
    /** Gets field names. */
    private static final int OP_GET_FIELD_NAMES = 7;

    /** Gets field types. */
    private static final int OP_GET_FIELDS_META = 8;

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
            List<GridQueryFieldMetadata> fieldsMeta = cursor().fieldsMeta();

            writer.writeInt(fieldsMeta.size());

            for (GridQueryFieldMetadata meta : fieldsMeta)
                writer.writeString(meta.fieldName());
        } else if (type == OP_GET_FIELDS_META) {
            List<GridQueryFieldMetadata> metas = cursor().fieldsMeta();

            if (metas == null) {
                writer.writeInt(0);
            } else {
                writer.writeInt(metas.size());

                for (GridQueryFieldMetadata meta : metas) {
                    writer.writeString(meta.fieldName());
                    writer.writeString(meta.fieldTypeName());
                }
            }
        } else {
            super.processOutStream(type, writer);
        }
    }
}
