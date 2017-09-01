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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.GridKernalContext;

/**
 * Gets binary type name by id.
 */
public class ClientGetBinaryTypeNameRequest extends ClientRequest {
    /** Platform ID, see org.apache.ignite.internal.MarshallerPlatformIds. */
    private final byte platformId;

    /** Type id. */
    private final int typeId;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    ClientGetBinaryTypeNameRequest(BinaryRawReader reader) {
        super(reader);

        platformId = reader.readByte();
        typeId = reader.readInt();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(GridKernalContext ctx) {
        try {
            String typeName = ctx.marshallerContext().getClassName(platformId, typeId);

            return new ClientGetBinaryTypeNameResponse(getRequestId(), typeName);
        } catch (ClassNotFoundException | IgniteCheckedException e) {
            throw new BinaryObjectException(e);
        }
    }
}
