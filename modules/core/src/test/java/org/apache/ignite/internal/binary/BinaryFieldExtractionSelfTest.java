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

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

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
     * Checking the exception and its text when changing the typeId of a
     * BinaryField.
     *
     * @throws Exception If failed.
     */
    public void testChangeTypeIdOfBinaryField() throws Exception {
        BinaryMarshaller marsh = createMarshaller();

        TimeValue timeVal = new TimeValue(11111L);
        DecimalValue decimalVal = new DecimalValue(BigDecimal.ZERO);

        BinaryObjectImpl timeValBinObj = toBinary(timeVal, marsh);
        BinaryObjectImpl decimalValBinObj = toBinary(decimalVal, marsh);

        BinaryFieldEx timeBinField = (BinaryFieldEx)timeValBinObj.type().field("time");

        Field typeIdField = U.findField(timeBinField.getClass(), "typeId");
        typeIdField.set(timeBinField, decimalValBinObj.typeId());

        String expMsg = exceptionMessageOfDifferentTypeIdBinaryField(
            decimalValBinObj.typeId(),
            decimalVal.getClass().getName(),
            timeValBinObj.typeId(),
            timeVal.getClass().getName(),
            U.field(timeBinField, "fieldId"),
            timeBinField.name(),
            null
        );

        assertThrows(log, () -> timeBinField.value(timeValBinObj), BinaryObjectException.class, expMsg);
    }

    /**
     * Checking the exception and its text when changing the typeId of a
     * BinaryField in case of not finding the expected BinaryType.
     *
     * @throws Exception If failed.
     */
    public void testChangeTypeIdOfBinaryFieldCaseNotFoundExpectedTypeId() throws Exception {
        BinaryMarshaller marsh = createMarshaller();

        TimeValue timeVal = new TimeValue(11111L);

        BinaryObjectImpl timeValBinObj = toBinary(timeVal, marsh);

        BinaryFieldEx timeBinField = (BinaryFieldEx)timeValBinObj.type().field("time");

        int newTypeId = timeValBinObj.typeId() + 1;

        Field typeIdField = U.findField(timeBinField.getClass(), "typeId");
        typeIdField.set(timeBinField, newTypeId);

        String expMsg = exceptionMessageOfDifferentTypeIdBinaryField(
            newTypeId,
            null,
            timeValBinObj.typeId(),
            timeVal.getClass().getName(),
            U.field(timeBinField, "fieldId"),
            timeBinField.name(),
            null
        );

        assertThrows(log, () -> timeBinField.value(timeValBinObj), BinaryObjectException.class, expMsg);
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

    /**
     * Creates an exception text for the case when the typeId differs in the
     * BinaryField and the BinaryObject.
     *
     * @param expTypeId Expected typeId.
     * @param expTypeName Expected typeName.
     * @param actualTypeId Actual typeId.
     * @param actualTypeName Actual typeName.
     * @param fieldId FieldId.
     * @param fieldName FieldName.
     * @param fieldType FieldType.
     * @return Exception message.
     */
    private String exceptionMessageOfDifferentTypeIdBinaryField(
        int expTypeId,
        String expTypeName,
        int actualTypeId,
        String actualTypeName,
        int fieldId,
        String fieldName,
        String fieldType
    ) {
        return "Failed to get field because type ID of passed object differs from type ID this " +
            "BinaryField belongs to [expected=[typeId=" + expTypeId + ", typeName=" + expTypeName +
            "], actual=[typeId=" + actualTypeId + ", typeName=" + actualTypeName + "], fieldId=" + fieldId +
            ", fieldName=" + fieldName + ", fieldType=" + fieldType + "]";
    }
}
