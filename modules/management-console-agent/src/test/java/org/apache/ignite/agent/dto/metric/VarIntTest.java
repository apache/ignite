/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.metric;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link VarIntWriter} and {@link VarIntReader} classes.
 */
public class VarIntTest {
    /** Initial capacity. */
    private static final int INITIAL_CAP = 16;

    /** Random generator. */
    private static final ThreadLocalRandom RND = ThreadLocalRandom.current();

    /**
     * Tests writing of boolean values.
     */
    @Test
    public void testPutBoolean() {
        testPutVarIntWriter(1, writer -> writer.putBoolean(RND.nextBoolean()));
    }

    /**
     * Tests writing of double values.
     */
    @Test
    public void testPutDouble() {
        testPutVarIntWriter(Double.BYTES, writer -> writer.putDouble(RND.nextDouble()));
    }

    /**
     * Tests writing of integer values in varInt format.
     */
    @Test
    public void testPutVarInt() {
        int maxBytes = 5;

        for (int i = 0; i < maxBytes; i++) {
            int lo = i == 0 ? 0 : 1 << (7 * i);

            int hi = i == maxBytes - 1 ? Integer.MAX_VALUE : 1 << (7 * (i + 1));

            testPutVarIntWriter(i + 1, writer -> writer.putVarInt(RND.nextInt(lo, hi)));
        }
    }

    /**
     * Tests writing of long values in varInt format.
     */
    @Test
    public void testPutVarLong() {
        int maxBytes = 10;

        for (int i = 0; i < maxBytes; i++) {
            long lo = i == 0 ? 0 : 1L << (7 * i);

            long hi = 1L << (7 * (i + 1));

            if (hi < 0)
                hi = Long.MAX_VALUE;

            long hi0 = hi;

            testPutVarIntWriter(i + 1, writer -> writer.putVarLong(RND.nextLong(lo, hi0)));
        }
    }

    /**
     * Tests writing of random values.
     */
    @Test
    public void testVarIntReadWriteRandomValues() {
        int valCnt = 1000;

        List<Object> vals = new ArrayList<>(valCnt);

        VarIntWriter writer = new VarIntWriter(INITIAL_CAP);

        for (int i = 0; i < valCnt; i++) {
            int type = RND.nextInt(4);

            switch (type) {
                case 0:
                    boolean booleanVal = RND.nextBoolean();

                    vals.add(booleanVal);

                    writer.putBoolean(booleanVal);

                    break;

                case 1:
                    double doubleVal = RND.nextDouble();

                    vals.add(doubleVal);

                    writer.putDouble(doubleVal);

                    break;

                case 2:
                    int intVal = RND.nextInt();

                    vals.add(intVal);

                    writer.putVarInt(intVal);

                    break;

                case 3:
                    long longVal = RND.nextLong();

                    vals.add(longVal);

                    writer.putVarLong(longVal);

                    break;

                default:
                    // No-op.
            }
        }

        byte[] arr = new byte[writer.position()];

        writer.toBytes(arr, 0);

        VarIntReader reader = new VarIntReader(arr);

        for (int i = 0; i < valCnt; i++) {
            Object o = vals.get(i);

            if (o instanceof Boolean)
                assertEquals(o, reader.getBoolean());
            else if (o instanceof Double)
                assertEquals(o, reader.getDouble());
            else if (o instanceof Integer)
                assertEquals(o, reader.getVarInt());
            else if (o instanceof Long)
                assertEquals(o, reader.getVarLong());
            else
                fail();
        }
    }

    /**
     * @param expSize Expected vale size in bytes.
     * @param consumer Consumer.
     */
    private void testPutVarIntWriter(int expSize, Consumer<VarIntWriter> consumer) {
        VarIntWriter writer = new VarIntWriter(INITIAL_CAP);

        assertEquals(0, writer.position());

        for (int i = 0; i < INITIAL_CAP / expSize + 1; i++) {
            consumer.accept(writer);

            assertEquals((i + 1) * expSize, writer.position());
        }

        assertEquals((INITIAL_CAP / expSize + 1) * expSize, writer.position());
    }
}
