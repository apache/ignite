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

package org.apache.ignite.internal.visor;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for data transfer objects for Visor tasks.
 *
 * @deprecated Use {@link IgniteDataTransferObject} instead. This class may be removed in Ignite 3.0.
 */
public abstract class VisorDataTransferObject implements Externalizable {
    /** */
    private static final long serialVersionUID = 6920203681702514010L;

    /** Magic number to detect correct transfer objects. */
    private static final int MAGIC = 0x42BEEF00;

    /** Version 1. */
    protected static final byte V1 = 1;

    /** Version 2. */
    protected static final byte V2 = 2;

    /** Version 3. */
    protected static final byte V3 = 3;

    /** Version 4. */
    protected static final byte V4 = 4;

    /**
     * @param col Source collection.
     * @param <T> Collection type.
     * @return List based on passed collection.
     */
    @Nullable protected static <T> List<T> toList(Collection<T> col) {
        if (col instanceof List)
            return (List<T>)col;

        if (col != null)
            return new ArrayList<>(col);

        return null;
    }

    /**
     * @param col Source collection.
     * @param <T> Collection type.
     * @return List based on passed collection.
     */
    @Nullable protected static <T> Set<T> toSet(Collection<T> col) {
        if (col != null)
            return new LinkedHashSet<>(col);

        return null;
    }

    /**
     * @return Transfer object version.
     */
    public byte getProtocolVersion() {
        return V1;
    }

    /**
     * Save object's specific data content.
     *
     * @param out Output object to write data content.
     * @throws IOException If I/O errors occur.
     */
    protected abstract void writeExternalData(ObjectOutput out) throws IOException;

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        int hdr = MAGIC + getProtocolVersion();

        out.writeInt(hdr);

        try (VisorDataTransferObjectOutput dtout = new VisorDataTransferObjectOutput(out)) {
            writeExternalData(dtout);
        }
    }

    /**
     * Load object's specific data content.
     *
     * @param protoVer Input object version.
     * @param in Input object to load data content.
     * @throws IOException If I/O errors occur.
     * @throws ClassNotFoundException If the class for an object being restored cannot be found.
     */
    protected abstract void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException;

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int hdr = in.readInt();

        if ((hdr & MAGIC) != MAGIC)
            throw new IOException("Unexpected VisorDataTransferObject header " +
                "[actual=" + Integer.toHexString(hdr) + ", expected=" + Integer.toHexString(MAGIC) + "]");

        byte ver = (byte)(hdr & 0xFF);

        try (VisorDataTransferObjectInput dtin = new VisorDataTransferObjectInput(in)) {
            readExternalData(ver, dtin);
        }
    }
}
