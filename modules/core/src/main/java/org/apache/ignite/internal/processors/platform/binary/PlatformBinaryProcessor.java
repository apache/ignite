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

package org.apache.ignite.internal.processors.platform.binary;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.MarshallerPlatformIds;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;

import java.util.HashMap;
import java.util.Map;

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

    /** */
    private static final int OP_REGISTER_TYPE = 5;

    /** */
    private static final int OP_GET_TYPE = 6;

    /** */
    private static final int OP_REGISTER_ENUM = 7;

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
        switch (type) {
            case OP_PUT_META:
                platformCtx.processMetadata(reader);

                return TRUE;

            case OP_REGISTER_TYPE: {
                int typeId = reader.readInt();
                String typeName = reader.readString();

                return platformContext().kernalContext().marshallerContext()
                    .registerClassName(MarshallerPlatformIds.DOTNET_ID, typeId, typeName, false)
                    ? TRUE : FALSE;
            }
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

            case OP_GET_TYPE: {
                int typeId = reader.readInt();

                try {
                    String typeName = platformContext().kernalContext().marshallerContext()
                        .getClassName(MarshallerPlatformIds.DOTNET_ID, typeId);

                    writer.writeString(typeName);
                }
                catch (ClassNotFoundException e) {
                    throw new BinaryObjectException(e);
                }

                break;
            }

            case OP_REGISTER_ENUM: {
                String name = reader.readString();

                int cnt = reader.readInt();

                Map<String, Integer> vals = new HashMap<>(cnt);

                for (int i = 0; i< cnt; i++) {
                    vals.put(reader.readString(), reader.readInt());
                }

                BinaryType binaryType = platformCtx.kernalContext().grid().binary().registerEnum(name, vals);

                platformCtx.writeMetadata(writer, binaryType.typeId());

                break;
            }

            default:
                super.processInStreamOutStream(type, reader, writer);
                break;
        }
    }
}
