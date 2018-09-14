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

package org.apache.ignite.internal.binary;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class BinaryFieldExtractionSelfTest extends GridCommonAbstractTest {
    /**
     * Create marshaller.
     *
     * @return Binary marshaller.
     * @throws Exception If failed.
     */
    protected BinaryMarshaller createMarshaller() throws Exception {
        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(),
            log());

        BinaryMarshaller marsh = new BinaryMarshaller();

        BinaryConfiguration bCfg = new BinaryConfiguration();

        IgniteConfiguration iCfg = new IgniteConfiguration();

        iCfg.setBinaryConfiguration(bCfg);

        marsh.setContext(new MarshallerContextTestImpl(null));

        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, iCfg);

        return marsh;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimitiveMarshalling() throws Exception {
        BinaryMarshaller marsh = createMarshaller();

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        TestObject obj = new TestObject(0);

        BinaryObjectImpl binObj = toBinary(obj, marsh);

        BinaryFieldEx[] fields = new BinaryFieldEx[] {
            (BinaryFieldEx)binObj.type().field("bVal"),
            (BinaryFieldEx)binObj.type().field("cVal"),
            (BinaryFieldEx)binObj.type().field("sVal"),
            (BinaryFieldEx)binObj.type().field("iVal"),
            (BinaryFieldEx)binObj.type().field("lVal"),
            (BinaryFieldEx)binObj.type().field("fVal"),
            (BinaryFieldEx)binObj.type().field("dVal")
        };

        ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);

        for (int i = 0; i < 100; i++) {
            TestObject to = new TestObject(rnd.nextLong());

            BinaryObjectImpl bObj = toBinary(to, marsh);

            for (BinaryFieldEx field : fields)
                field.writeField(bObj, buf);

            buf.flip();

            for (BinaryFieldEx field : fields)
                assertEquals(field.value(bObj), field.readField(buf));

            buf.flip();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimeMarshalling() throws Exception {
        BinaryMarshaller marsh = createMarshaller();

        TimeValue obj = new TimeValue(11111L);

        BinaryObjectImpl binObj = toBinary(obj, marsh);

        BinaryFieldEx field = (BinaryFieldEx)binObj.type().field("time");

        ByteBuffer buf = ByteBuffer.allocate(16);

        field.writeField(binObj, buf);

        buf.flip();

        assertEquals(field.value(binObj), field.<Time>readField(buf));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecimalFieldMarshalling() throws Exception {
        BinaryMarshaller marsh = createMarshaller();

        BigDecimal values[] = new BigDecimal[] { BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.TEN,
            new BigDecimal("-100.5"), BigDecimal.valueOf(Long.MAX_VALUE, 0),
            BigDecimal.valueOf(Long.MIN_VALUE, 0), BigDecimal.valueOf(Long.MAX_VALUE, 8),
            BigDecimal.valueOf(Long.MIN_VALUE, 8)};

        DecimalValue decVal = new DecimalValue(values[0]);

        BinaryObjectImpl binObj = toBinary(decVal, marsh);

        BinaryFieldEx field = (BinaryFieldEx)binObj.type().field("decVal");

        ByteBuffer buf = ByteBuffer.allocate(64);

        for (BigDecimal value : values) {
            decVal = new DecimalValue(value);

            binObj = toBinary(decVal, marsh);

            field.writeField(binObj, buf);

            buf.flip();

            assertEquals(field.value(binObj), field.readField(buf));

            buf.clear();
        }
    }

    /**
     * @param obj Object to transform to a binary object.
     * @param marsh Binary marshaller.
     * @return Binary object.
     */
    protected BinaryObjectImpl toBinary(Object obj, BinaryMarshaller marsh) throws Exception {
        byte[] bytes = marsh.marshal(obj);

        return new BinaryObjectImpl(binaryContext(marsh), bytes, 0);
    }

    /**
     * Get binary context for the current marshaller.
     *
     * @param marsh Marshaller.
     * @return Binary context.
     */
    protected static BinaryContext binaryContext(BinaryMarshaller marsh) {
        GridBinaryMarshaller impl = U.field(marsh, "impl");

        return impl.context();
    }

    /**
     *
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class TestObject {
        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int iVal;

        /** */
        private long lVal;

        /** */
        private float fVal;

        /** */
        private double dVal;

        /**
         * @param seed Seed.
         */
        private TestObject(long seed) {
            bVal = (byte)seed;
            cVal = (char)seed;
            sVal = (short)seed;
            iVal = (int)seed;
            lVal = seed;
            fVal = seed;
            dVal = seed;
        }
    }

    /** */
    private static class TimeValue {
        /** */
        private Time time;

        /**
         * @param time Time.
         */
        TimeValue(long time) {
            this.time = new Time(time);
        }
    }

    /**
     *
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class DecimalValue {
        /** */
        private BigDecimal decVal;

        /**
         *
         * @param decVal Value to use
         */
        private DecimalValue(BigDecimal decVal) {
            this.decVal = decVal;
        }
    }
}
