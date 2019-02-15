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

package org.apache.ignite.internal.visor;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import org.apache.ignite.internal.dto.IgniteDataTransferObjectOutput;
import org.apache.ignite.internal.util.io.GridByteArrayOutputStream;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Wrapper for object output.
 * @deprecated Use {@link IgniteDataTransferObjectOutput} instead. This class may be removed in Ignite 3.0.
 */
public class VisorDataTransferObjectOutput implements ObjectOutput {
    /** */
    private final ObjectOutput out;

    /** */
    private final GridByteArrayOutputStream bos;

    /** */
    private final ObjectOutputStream oos;

    /**
     * Constructor.
     *
     * @param out Target stream.
     * @throws IOException If an I/O error occurs.
     */
    public VisorDataTransferObjectOutput(ObjectOutput out) throws IOException {
        this.out = out;

        bos = new GridByteArrayOutputStream();
        oos = new ObjectOutputStream(bos);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(Object obj) throws IOException {
        oos.writeObject(obj);
    }

    /** {@inheritDoc} */
    @Override public void write(int b) throws IOException {
        oos.write(b);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b) throws IOException {
        oos.write(b);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b, int off, int len) throws IOException {
        oos.write(b, off, len);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean v) throws IOException {
        oos.writeBoolean(v);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(int v) throws IOException {
        oos.writeByte(v);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int v) throws IOException {
        oos.writeShort(v);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(int v) throws IOException {
        oos.writeChar(v);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int v) throws IOException {
        oos.writeInt(v);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long v) throws IOException {
        oos.writeLong(v);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float v) throws IOException {
        oos.writeFloat(v);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double v) throws IOException {
        oos.writeDouble(v);
    }

    /** {@inheritDoc} */
    @Override public void writeBytes(@NotNull String s) throws IOException {
        oos.writeBytes(s);
    }

    /** {@inheritDoc} */
    @Override public void writeChars(@NotNull String s) throws IOException {
        oos.writeChars(s);
    }

    /** {@inheritDoc} */
    @Override public void writeUTF(@NotNull String s) throws IOException {
        oos.writeUTF(s);
    }

    /** {@inheritDoc} */
    @Override public void flush() throws IOException {
        oos.flush();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        oos.flush();

        U.writeByteArray(out, bos.internalArray(), bos.size());

        oos.close();
    }
}
