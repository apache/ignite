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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.crc;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferExpander;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SimpleFileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class IgniteDataIntegrityTests {
    /** File input. */
    private SimpleFileInput fileInput;

    /** Buffer expander. */
    private ByteBufferExpander expBuf;

    /** */
    @Before
    public void setUp() throws Exception {
        File file = File.createTempFile("integrity", "dat");
        file.deleteOnExit();

        expBuf = new ByteBufferExpander(1024, ByteOrder.BIG_ENDIAN);

        FileIOFactory factory = new RandomAccessFileIOFactory();

        fileInput = new SimpleFileInput(
                factory.create(file),
                expBuf
        );

        ByteBuffer buf = ByteBuffer.allocate(1024);

        ThreadLocalRandom curr = ThreadLocalRandom.current();

        for (int i = 0; i < 1024; i+=16) {
            buf.putInt(curr.nextInt());
            buf.putInt(curr.nextInt());
            buf.putInt(curr.nextInt());
            buf.position(i);
            buf.putInt(FastCrc.calcCrc(buf, 12));
        }

        buf.rewind();

        fileInput.io().writeFully(buf);
        fileInput.io().force();
    }

    /** */
    @After
    public void tearDown() throws Exception {
        fileInput.io().close();
        expBuf.close();
    }

    /**
     *
     */
    @Test
    public void testSuccessfulPath() throws Exception {
        checkIntegrity();
    }

    /**
     *
     */
    @Test
    public void testIntegrityViolationChecking() throws Exception {
        toggleOneRandomBit(0, 1024 - 16);

        try {
            checkIntegrity();

            fail();
        } catch (IgniteDataIntegrityViolationException ex) {
            //success
        }
    }

    /**
     *
     */
    @Test
    public void testSkipingLastCorruptedEntry() throws Exception {
        toggleOneRandomBit(1024 - 16, 1024);

        try {
            checkIntegrity();

            fail();
        } catch (EOFException ex) {
            //success
        }
    }

    /**
     *
     */
    @Test
    public void testExpandBuffer() {
        ByteBufferExpander expBuf = new ByteBufferExpander(24, ByteOrder.nativeOrder());

        ByteBuffer b1 = expBuf.buffer();

        b1.put((byte)1);
        b1.putInt(2);
        b1.putLong(3L);

        assertEquals(13, b1.position());
        assertEquals(24, b1.limit());

        ByteBuffer b2 = expBuf.expand(32);

        assertEquals(13, b2.position());
        assertEquals(24, b2.limit());

        b2.rewind();

        assertEquals(0, b2.position());
        assertEquals((byte)1, b2.get());
        assertEquals(2, b2.getInt());
        assertEquals(3L, b2.getLong());
        assertEquals(13, b2.position());
        assertEquals(24, b2.limit());
        assertEquals(32, b2.capacity());

        b2.limit(b2.capacity());

        b2.putInt(4);
        b2.putInt(5);
        b2.putInt(6);

        assertEquals(25, b2.position());
        assertEquals(32, b2.limit());
        assertEquals(32, b2.capacity());

        b2.flip();

        assertEquals(0, b2.position());
        assertEquals((byte)1, b2.get());
        assertEquals(2, b2.getInt());
        assertEquals(3L, b2.getLong());
        assertEquals(4, b2.getInt());
        assertEquals(5, b2.getInt());
        assertEquals(6, b2.getInt());
        assertEquals(25, b2.limit());
        assertEquals(32, b2.capacity());
    }

    /**
     * @param rangeFrom Range from.
     * @param rangeTo Range to.
     */
    private void toggleOneRandomBit(int rangeFrom, int rangeTo) throws IOException {
        int pos = ThreadLocalRandom.current().nextInt(rangeFrom, rangeTo);
        fileInput.io().position(pos);

        byte[] buf = new byte[1];

        fileInput.io().readFully(buf, 0, 1);

        buf[0] ^= (1 << 3);

        fileInput.io().position(pos);
        fileInput.io().writeFully(buf, 0, 1);
        fileInput.io().force();
    }

    /**
     *
     */
    private void checkIntegrity() throws Exception {
        fileInput.io().position(0);

        for (int i = 0; i < 1024 / 16; i++) {
            try(FileInput.Crc32CheckingFileInput in = fileInput.startRead(false)) {
                in.readInt();
                in.readInt();
                in.readInt();
            }
        }
    }
}
