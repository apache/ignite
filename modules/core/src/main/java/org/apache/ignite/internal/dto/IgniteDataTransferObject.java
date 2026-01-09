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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for data transfer objects.
 */
public abstract class IgniteDataTransferObject implements Externalizable {
    /** Serial version UUID. */
    private static final long serialVersionUID = 0L;

    /** Magic number to detect correct transfer objects. */
    private static final int MAGIC = 0x42BEEF00;

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
     * Save object's specific data content.
     *
     * @param out Output object to write data content.
     * @throws IOException If I/O errors occur.
     */
    protected abstract void writeExternalData(ObjectOutput out) throws IOException;

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(MAGIC);

        try (IgniteDataTransferObjectOutput dtout = new IgniteDataTransferObjectOutput(out)) {
            writeExternalData(dtout);
        }
    }

    /**
     * Load object's specific data content.
     *
     * @param in Input object to load data content.
     * @throws IOException If I/O errors occur.
     * @throws ClassNotFoundException If the class for an object being restored cannot be found.
     */
    protected abstract void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException;

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int hdr = in.readInt();

        if ((hdr & MAGIC) != MAGIC)
            throw new IOException("Unexpected IgniteDataTransferObject header " +
                "[actual=" + Integer.toHexString(hdr) + ", expected=" + Integer.toHexString(MAGIC) + "]");

        try (IgniteDataTransferObjectInput dtin = new IgniteDataTransferObjectInput(in)) {
            readExternalData(dtin);
        }
    }
}
