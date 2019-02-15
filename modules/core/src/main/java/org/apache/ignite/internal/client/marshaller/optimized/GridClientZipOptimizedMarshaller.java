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

package org.apache.ignite.internal.client.marshaller.optimized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.client.marshaller.GridClientMarshaller;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.processors.rest.client.message.GridClientMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.plugin.PluginProvider;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper, that adapts {@link OptimizedMarshaller} to {@link GridClientMarshaller} interface.
 */
public class GridClientZipOptimizedMarshaller extends GridClientOptimizedMarshaller {
    /** ID. */
    public static final byte ID = 3;

    /** Default buffer size. */
    private static final int DFLT_BUFFER_SIZE = 4096;

    /** Default client marshaller to fallback. */
    private final GridClientMarshaller dfltMarsh;

    /**
     * Constructor.
     *
     * @param dfltMarsh Marshaller to fallback to.
     * @param plugins Plugins.
     */
    public GridClientZipOptimizedMarshaller(GridClientMarshaller dfltMarsh, @Nullable List<PluginProvider> plugins) {
        super(plugins);

        assert dfltMarsh!= null;

        this.dfltMarsh = dfltMarsh;
    }

    /**
     * Default marshaller that will be used in case of backward compatibility.
     *
     * @return Marshaller to fallback.
     */
    public GridClientMarshaller defaultMarshaller() {
        return dfltMarsh;
    }

    /**
     * Zips bytes.
     *
     * @param input Input bytes.
     * @return Zipped byte array.
     * @throws IOException If failed.
     */
    public static byte[] zipBytes(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(DFLT_BUFFER_SIZE);

        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry = new ZipEntry("");

            try {
                entry.setSize(input.length);

                zos.putNextEntry(entry);
                zos.write(input);
            }
            finally {
                zos.closeEntry();
            }
        }

        return baos.toByteArray();
    }

    /**
     * Unzip bytes.
     *
     * @param input Zipped bytes.
     * @return Unzipped byte array.
     * @throws IOException
     */
    private static byte[] unzipBytes(byte[] input) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(input);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(DFLT_BUFFER_SIZE);

        try(ZipInputStream zis = new ZipInputStream(bais)) {
            zis.getNextEntry();

            byte[] buf = new byte[DFLT_BUFFER_SIZE];

            int len = zis.read(buf);

            while (len > 0) {
                baos.write(buf, 0, len);

                len = zis.read(buf);
            }
        }

        return baos.toByteArray();
    }
    /** {@inheritDoc} */
    @Override public ByteBuffer marshal(Object obj, int off) throws IOException {
        try {
            if (!(obj instanceof GridClientMessage))
                throw new IOException("Message serialization of given type is not supported: " +
                    obj.getClass().getName());

            byte[] marshBytes = U.marshal(opMarsh, obj);

            boolean zip = marshBytes.length > 512;

            byte[] bytes = zip ? zipBytes(marshBytes) : marshBytes;

            ByteBuffer buf = ByteBuffer.allocate(off + bytes.length + 1);

            buf.position(off);
            buf.put((byte)(zip ? 1 : 0));
            buf.put(bytes);
            buf.flip();

            return buf;
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] bytes) throws IOException {
        try {
            boolean unzip = bytes[0] > 0;

            byte[] marshBytes = Arrays.copyOfRange(bytes, 1, bytes.length);

            return U.unmarshal(opMarsh, unzip ? unzipBytes(marshBytes) : marshBytes, null);
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }
}
