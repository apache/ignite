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

package org.apache.ignite.internal.dto;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.codegen.idto.IDTOSerializerFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for data transfer objects.
 */
public abstract class IgniteDataTransferObject implements Externalizable {
    /** Serial version UUID. */
    private static final long serialVersionUID = 0L;

    /** Magic number to detect correct transfer objects. */
    private static final int MAGIC = 0xBAA55F5E;

    /**
     * @param col Source collection.
     * @param <T> Collection type.
     * @return List based on passed collection.
     */
    @Nullable protected static <T> List<T> toList(Collection<T> col) {
        if (col != null)
            return new ArrayList<>(col);

        return null;
    }

    /** {@inheritDoc} */
    @Override public final void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(MAGIC);

        writeIgniteDataTransferObject(out);
    }

    /** {@inheritDoc} */
    @Override public final void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int hdr = in.readInt();

        if ((hdr & MAGIC) != MAGIC)
            throw new IOException("Unexpected Ignite DTO message header. The input stream is malformed or was generated " +
                "by an incompatible Ignite version [actual=" + Integer.toHexString(hdr) +
                    ", expected=" + Integer.toHexString(MAGIC) + ']'
            );

        readIgniteDataTransferObject(in);
    }

    /** */
    protected void writeIgniteDataTransferObject(ObjectOutput out) throws IOException {
        try (IgniteDataTransferObjectOutput dtout = new IgniteDataTransferObjectOutput(out)) {
            IgniteDataTransferObjectSerializer serializer = IDTOSerializerFactory.getInstance().serializer(getClass());

            serializer.writeExternal(this, dtout);
        }
    }

    /** */
    protected void readIgniteDataTransferObject(ObjectInput in) throws IOException, ClassNotFoundException {
        try (IgniteDataTransferObjectInput dtin = new IgniteDataTransferObjectInput(in)) {
            IgniteDataTransferObjectSerializer serializer = IDTOSerializerFactory.getInstance().serializer(getClass());

            serializer.readExternal(this, dtin);
        }
    }
}
