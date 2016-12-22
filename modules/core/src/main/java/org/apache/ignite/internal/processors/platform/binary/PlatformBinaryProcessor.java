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

package org.apache.ignite.internal.processors.platform.binary;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;

/**
 * Platform binary processor.
 */
public class PlatformBinaryProcessor extends PlatformAbstractTarget {
    /** */
    private static final int OP_GET_META = 1;

    /** */
    private static final int OP_GET_ALL_META = 2;

    /** */
    private static final int OP_PUT_META = 3;

    /** */
    private static final int OP_GET_SCHEMA = 4;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     */
    public PlatformBinaryProcessor(PlatformContext platformCtx) {
        super(platformCtx);
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        if (type == OP_PUT_META) {
            platformCtx.processMetadata(reader);

            return TRUE;
        }

        return super.processInStreamOutLong(type, reader);
    }

    /** {@inheritDoc} */
    @Override public void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException {
        if (type == OP_GET_ALL_META)
            platformCtx.writeAllMetadata(writer);
        else
            super.processOutStream(type, writer);
    }

    /** {@inheritDoc} */
    @Override public void processInStreamOutStream(int type, BinaryRawReaderEx reader,
        BinaryRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_GET_META: {
                int typeId = reader.readInt();

                platformCtx.writeMetadata(writer, typeId);

                break;
            }

            case OP_GET_SCHEMA: {
                int typeId = reader.readInt();
                int schemaId = reader.readInt();

                platformCtx.writeSchema(writer, typeId, schemaId);

                break;
            }

            default:
                super.processInStreamOutStream(type, reader, writer);
                break;
        }
    }
}
