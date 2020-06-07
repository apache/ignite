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

import com.sun.tools.javac.util.Pair;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.MarshallerTestObjects.ComplexObject;
import org.apache.ignite.internal.binary.MarshallerTestObjects.EmptyObject;
import org.apache.ignite.internal.binary.MarshallerTestObjects.LargeNestedComplexObject;
import org.apache.ignite.internal.binary.MarshallerTestObjects.NestedComplexObject;
import org.apache.ignite.internal.binary.MarshallerTestObjects.StartingWithNull;
import org.apache.ignite.internal.binary.MarshallerTestObjects.VerySimpleObject;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

@SuppressWarnings({"OverlyStrongTypeCast", "ConstantConditions"})
public class BinaryMarshallerCompactNullsSelfTest extends BinaryMarshallerSelfTest {

    public static final String INDENT_SUB_OBJECT = "*** ";

    @Override protected boolean compactNull() {
        return true;
    }

    static Pair<Object, int[]>[] testCasesNoCompactFooterNoNullCompaction = new Pair[]{
            new Pair(new VerySimpleObject(),
                    new int[]{88, 53}), //Length, footer position, footer length
            new Pair(new StartingWithNull(),
                    new int[]{88, 53}), //Length, footer position, footer
            new Pair(new ComplexObject(),
                    new int[]{110, 70}), //Length, footer position, footer length
            new Pair(new NestedComplexObject(),
                    new int[]{181, 141}), //Length, footer position, footer length
            new Pair(new EmptyObject(),
                    new int[]{24, 0}), //Length, footer position, footer length
            new Pair(new LargeNestedComplexObject(),
                    new int[]{407, 352}) //Length, footer position, footer length
    };

    static Pair<Object, int[]>[] testCasesCompactFooterNoNullCompaction = new Pair[]{
            new Pair(new VerySimpleObject(),
                    new int[]{60, 53}), //Length, footer position, footer length
            new Pair(new StartingWithNull(),
                    new int[]{60, 53}), //Length, footer position, footer length
            new Pair(new ComplexObject(),
                    new int[]{78, 70}), //Length, footer position, footer length
            new Pair(new NestedComplexObject(),
                    new int[]{121, 113}), //Length, footer position, footer length
            new Pair(new EmptyObject(),
                    new int[]{24, 0}), //Length, footer position, footer length,
            new Pair(new LargeNestedComplexObject(),
                    new int[]{307, 296}) //Length, footer position, footer length
    };

    static Pair<Object, int[]>[] testCasesCompactFooterNullCompaction = new Pair[]{
            new Pair(new VerySimpleObject(),
                    new int[]{55, 50}), //Length, footer position, footer length
            new Pair(new StartingWithNull(),
                    new int[]{55, 50}), //Length, footer position, footer length
            new Pair(new ComplexObject(),
                    new int[]{73, 67}), //Length, footer position, footer length
            new Pair(new NestedComplexObject(),
                    new int[]{111, 105}), //Length, footer position, footer length
            new Pair(new EmptyObject(),
                    new int[]{24, 0}), //Length, footer position, footer length,
            new Pair(new LargeNestedComplexObject(),
                    new int[]{293, 283}) //Length, footer position, footer length
    };

    @Test
    public void testComputeNotNullFields() {
        int absolutePos = 10;
        byte[] nullMask = new byte[]{(byte) 0xE7, (byte) 0x05};
        int realPos = BinaryReaderExImpl.computeCountOfNotNullFields(nullMask, absolutePos);
        assertEquals(7, realPos);

        absolutePos = 7;
        nullMask = new byte[]{(byte) 0xE7, (byte) 0x05};
        realPos = BinaryReaderExImpl.computeCountOfNotNullFields(nullMask, absolutePos);
        assertEquals(5, realPos);

        absolutePos = 8;
        nullMask = new byte[]{(byte) 0xE7, (byte) 0x05};
        realPos = BinaryReaderExImpl.computeCountOfNotNullFields(nullMask, absolutePos);
        assertEquals(6, realPos);

        absolutePos = 0;
        nullMask = new byte[]{(byte) 0xE7, (byte) 0x05};
        realPos = BinaryReaderExImpl.computeCountOfNotNullFields(nullMask, absolutePos);
        assertEquals(0, realPos);

        absolutePos = 7;
        nullMask = new byte[]{(byte) 0xE7};
        realPos = BinaryReaderExImpl.computeCountOfNotNullFields(nullMask, absolutePos);
        assertEquals(5, realPos);

        absolutePos = 7;
        nullMask = new byte[]{(byte) 0xF7};
        realPos = BinaryReaderExImpl.computeCountOfNotNullFields(nullMask, absolutePos);
        assertEquals(6, realPos);

        absolutePos = 0;
        nullMask = new byte[]{(byte) 0xF7};
        realPos = BinaryReaderExImpl.computeCountOfNotNullFields(nullMask, absolutePos);
        assertEquals(0, realPos);
    }

    @Test
    public void testIsNullFields() {
        int absolutePos = 10;
        byte[] nullMask = new byte[]{(byte) 0xE7, (byte) 0x05};
        assertEquals(false, BinaryReaderExImpl.isFieldNull(nullMask, absolutePos));

        absolutePos = 7;
        nullMask = new byte[]{(byte) 0xE7, (byte) 0x05};
        assertEquals(false, BinaryReaderExImpl.isFieldNull(nullMask, absolutePos));

        absolutePos = 8;
        nullMask = new byte[]{(byte) 0xE7, (byte) 0x05};
        assertEquals(false, BinaryReaderExImpl.isFieldNull(nullMask, absolutePos));

        absolutePos = 0;
        nullMask = new byte[]{(byte) 0xE7, (byte) 0x05};
        assertEquals(false, BinaryReaderExImpl.isFieldNull(nullMask, absolutePos));

        absolutePos = 7;
        nullMask = new byte[]{(byte) 0xE7};
        assertEquals(false, BinaryReaderExImpl.isFieldNull(nullMask, absolutePos));

        absolutePos = 3;
        nullMask = new byte[]{(byte) 0xE7};
        assertEquals(true, BinaryReaderExImpl.isFieldNull(nullMask, absolutePos));

        absolutePos = 7;
        nullMask = new byte[]{(byte) 0xF7};
        assertEquals(false, BinaryReaderExImpl.isFieldNull(nullMask, absolutePos));

        absolutePos = 0;
        nullMask = new byte[]{(byte) 0xF7};
        assertEquals(false, BinaryReaderExImpl.isFieldNull(nullMask, absolutePos));

        absolutePos = 0;
        nullMask = new byte[]{(byte) 0xF0};
        assertEquals(true, BinaryReaderExImpl.isFieldNull(nullMask, absolutePos));
    }

    @Test
    public void testCompactFooterAndCompactNullDisabled() throws Exception {
        BinaryMarshaller marsh = createMarshaller(false, false);
        testMarshallerConfiguration(marsh, testCasesNoCompactFooterNoNullCompaction);
    }

    @Test
    public void testCompactFooterEnabledAndCompactNullDisabled() throws Exception {
        BinaryMarshaller marsh = createMarshaller(true, false);
        testMarshallerConfiguration(marsh, testCasesCompactFooterNoNullCompaction);
    }

    @Test
    public void testCompactFooterEnabledAndCompactNullEnabled() throws Exception {
        BinaryMarshaller marsh = createMarshaller(true, true);
        testMarshallerConfiguration(marsh, testCasesCompactFooterNullCompaction);
    }

    @Test(expected = BinaryObjectException.class)
    public void testCompactFooterDisabledAndCompactNullDisabled() throws Exception {
        BinaryMarshaller marsh = createMarshaller(false, true);
        byte[] boimpl = marsh.marshal(new VerySimpleObject());
    }

    @Test
    public void testCompressionFactorWithCompactNull() throws Exception {
        BinaryMarshaller marshCompactNullEnabled = createMarshaller(true, true);
        BinaryMarshaller marshCompactNullDisabled = createMarshaller(true, false);

        Object[] objectsToTest = new Object[]{new VerySimpleObject(), new NestedComplexObject(), new StartingWithNull(),
                new ComplexObject(), new LargeNestedComplexObject(), new MarshallerTestObjects.ObjectwithLotsOfNull()};

        for (Object o : objectsToTest) {
            byte[] compactNullDisabled = marshCompactNullDisabled.marshal(o);
            byte[] compactNullEnabled = marshCompactNullEnabled.marshal(o);
            double compressedFactor = ((double) compactNullEnabled.length) * 100 / compactNullDisabled.length;
            System.out.println(String.format("Object=%s\nNon-null compacted size=%d\nNull compacted Size=%d Gain=%.2f%%\n", o.getClass().getSimpleName(),
                    compactNullDisabled.length, compactNullEnabled.length,
                    100 - compressedFactor)
            );
        }
    }

    /**
     * Create marshaller.
     *
     * @return Binary marshaller.
     * @throws Exception If failed.
     */
    protected BinaryMarshaller createMarshaller(boolean compactFooter, boolean compactNull) throws Exception {
        return createMarshaller(compactFooter, compactNull, null);
    }

    /**
     * Create marshaller.
     *
     * @return Binary marshaller.
     * @throws Exception If failed.
     */
    protected BinaryMarshaller createMarshaller(boolean compactFooter, boolean compactNull, Collection<BinaryTypeConfiguration> cfgs) throws Exception {
        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(),
                new IgniteConfiguration(),
                log());

        BinaryMarshaller marsh = new BinaryMarshaller();
        BinaryConfiguration bCfg = new BinaryConfiguration().setCompactFooter(compactFooter).setCompactNulls(compactNull);

        if (cfgs != null) {
            bCfg.setTypeConfigurations(cfgs);
        }
        IgniteConfiguration iCfg = new IgniteConfiguration();
        iCfg.setBinaryConfiguration(bCfg);
        marsh.setContext(new MarshallerContextTestImpl(null));
        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, iCfg);
        return marsh;
    }

    /**
     * Prints out a string representation of the binary object for debug purposes
     * @param bo to print out.
     * @return a StringBuilder containing the representation
     */
    private static StringBuilder byteArrayToHex(BinaryObjectImpl bo) {
        byte[] a = bo.array();
        return byteArrayToHex(a, bo, 0);
    }

    /**
     * Prints out a string representation of the binary object for debug purposes
     * @param a an array of bytes
     * @param bo the binary object
     * @param level level of nested objects
     * @return a StringBuilder containing the representation
     */
    private static StringBuilder byteArrayToHex(byte[] a, BinaryObjectImpl bo, int level) {

        StringBuilder indentString = buildIndentString(level);
        StringBuilder sb = new StringBuilder(a.length * 2);
        try {

            int footerPosition = getFooterPosition(a);
            int fieldOffsetLength = BinaryUtils.fieldOffsetLength(a[2]);

            sb.append(indentString).append(String.format("0x%02x // ValueType\n", a[0]));
            sb.append(indentString).append(String.format("0x%02x // FormatVersion \n", a[1]));
            boolean isCompactFooter = BinaryUtils.isCompactFooter(a[2]);
            sb.append(indentString).append(String.format("0x%02x 0x%02x //Flags userType=%b hasSchema=%b rawData=%b offset=%d compactFooter=%b compactNull=%b\n", a[2], a[3],
                BinaryUtils.isUserType(a[2]),
                BinaryUtils.hasSchema(a[2]),
                BinaryUtils.hasRaw(a[2]),
                fieldOffsetLength,
                isCompactFooter,
                BinaryUtils.isCompactNull(a[2])));
            sb.append(indentString).append(String.format("0x%02x 0x%02x 0x%02x 0x%02x //TypeId \n", a[4], a[5], a[6], a[7]));
            sb.append(indentString).append(String.format("0x%02x 0x%02x 0x%02x 0x%02x //Hashcode \n", a[8], a[9], a[10], a[11]));
            sb.append(indentString).append(String.format("0x%02x 0x%02x 0x%02x 0x%02x //Length \n", a[12], a[13], a[14], a[15]));
            sb.append(indentString).append(String.format("0x%02x 0x%02x 0x%02x 0x%02x //SchemaId \n", a[16], a[17], a[18], a[19]));
            sb.append(indentString).append(String.format("0x%02x 0x%02x 0x%02x 0x%02x //Footer position = %d \n", a[20], a[21], a[22], a[23], footerPosition));

            int typeId = (a[4] & 0xff ) + ((a[5] & 0xff) << 8) + ((a[6] & 0xff) << 16) + ((a[7] & 0xff) << 24);
            BinaryClassDescriptor desc = bo.context().descriptorForTypeId(BinaryUtils.isUserType(a[2]), typeId, null, false);
            sb.append(indentString).append(String.format("Class = %s\n", desc.typeName()));
            int nbFields = desc == null || desc.fieldsMeta() == null ? 0 : desc.fieldsMeta().size();
            sb.append(indentString).append(String.format("nbFields = %d (0 might mean unknown)   ", nbFields));
            sb.append(indentString).append(String.format("SchemaId = %d", typeId));

            sb.append("\n");
            sb.append(indentString).append("**************** BODY *************************\n");
            sb.append(indentString);
            for (int i = 24; i < footerPosition; i++)
                sb.append(String.format("0x%02x ", a[i]));
            sb.append("\n");
            sb.append(indentString).append("**************** BODY *************************\n");

            sb.append(indentString).append(String.format("Footer length=%d\n", a.length - footerPosition));
            sb.append(indentString).append("**************** FOOTER *************************\n");
            sb.append(indentString);
            for (int i = footerPosition; i < a.length; i++)
                sb.append(String.format("0x%02x ", a[i]));
            sb.append("\n");
            sb.append(indentString).append("**************** FOOTER *************************\n");

            if (BinaryUtils.isCompactNull(a[2])) {
                int rawOffSetLength = BinaryUtils.hasRaw(a[2]) ? 4 : 0;
                byte[] nullMask = Arrays.copyOfRange(a,
                    a.length - rawOffSetLength - (nbFields / 8) + (nbFields % 8 != 0 ? 1 : 0),
                    a.length - rawOffSetLength);

                sb.append(indentString).append("Null Mask : ");
                for (int i = 0; i < nullMask.length; i++) {
                    sb.append(Integer.toBinaryString((nullMask[i] & 0xFF) + 256).substring(1));
                    sb.append(" ");
                }
                sb.append("\n");
                int start = footerPosition;
                int end = -1;

                for (int i = 0; i < nbFields; i++) {
                    if (!BinaryReaderExImpl.isFieldNull(nullMask, i)) {
                        start = printBinaryField(a, bo, desc, sb, footerPosition,
                            fieldOffsetLength, start, i, level, nullMask.length);
                    } else {
                        sb.append(indentString).append(String.format("Field %d %s IS NULL \n", i,
                                desc.fieldsMeta() != null ? desc.fieldsMeta().keySet().toArray()[i] : ""));
                    };
                }
            } else {
                int startPosInFooter = footerPosition;
                int end = -1;

                for (int i = 0; i < nbFields; i++) {
                    startPosInFooter = printBinaryField(a, bo, desc, sb, footerPosition,
                        fieldOffsetLength, startPosInFooter, i, level, 0);
                }
            }

        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
        } finally {
            return sb;
        }
    }

    @NotNull private static StringBuilder buildIndentString(int level) {
        StringBuilder indentString = new StringBuilder();
        for (int i = 0; i < level; i++) { indentString.append(INDENT_SUB_OBJECT); }
        return indentString;
    }

    private static int printBinaryField(byte[] a, BinaryObjectImpl bo, BinaryClassDescriptor desc, StringBuilder sb,
        int footerPosition, int fieldOffsetLength, int startPosInFooter, int fieldPos, int level, int nullMaskLen) {
        int end;
        int startAddress = -1;
        int offsetOfLongFooter = BinaryUtils.isCompactFooter(a[2]) ? 0 : 4;
        startAddress = readAddressOfField(a, startPosInFooter + offsetOfLongFooter, fieldOffsetLength);
        end = footerPosition;
        int nextStartPosInFooter = startPosInFooter + 2 * offsetOfLongFooter + fieldOffsetLength;
        StringBuilder indentString = buildIndentString(level);

        if ((nextStartPosInFooter + fieldOffsetLength) <= (a.length - nullMaskLen) ) {
            end = readAddressOfField(a, nextStartPosInFooter,
                fieldOffsetLength);
            if (end > footerPosition) {
                end = footerPosition;
            }
        }
        sb.append(indentString).append(String.format("Field %d. from: %d to: %d ==> %s ", fieldPos,
            startAddress, end, desc.fieldsMeta() != null ? desc.fieldsMeta().keySet().toArray()[fieldPos] : ""));
        for (int j = startAddress; j < end; j++) {
            sb.append(String.format("0x%02x ", a[j]));
        }
        if (a[startAddress] == 0x67) {
            sb.append("\n");
            sb.append(byteArrayToHex(Arrays.copyOfRange(a, startAddress, end), bo, level + 1));
        }

        sb.append("\n");
        startPosInFooter = startPosInFooter + fieldOffsetLength + offsetOfLongFooter;
        return startPosInFooter;
    }

    private static int readAddressOfField(byte[] a, int start, int offsetLength) {
       int add = 0;
       for (int i = 0; i < offsetLength; i = i + 1) {
            add = add + (Byte.toUnsignedInt(a[start + i]) << (8 * i));
       }
       return add;
    }

    private static int getFooterPosition(byte[] a) {
        return ((int) (a[23] & 0xFF) << 24) + ((int) (a[22] & 0xFF) << 16) + ((int) (a[21] & 0xFF) << 8) + ((int) a[20] & 0xFF);
    }

    private static int getFooterLength(byte[] a) {
        return a.length - getFooterPosition(a);
    }

    private void testMarshallerConfiguration(BinaryMarshaller marsh, Pair<Object, int[]>[] testCases) throws IgniteCheckedException {
        for (Pair<Object, int[]> testCase : testCases) {
            System.out.println(testCase.fst.getClass());
            BinaryObjectImpl po = marshal(testCase.fst, marsh);
            System.out.println(byteArrayToHex(po));
            assertEquals(testCase.snd[0], po.array().length);
            assertEquals(testCase.snd[1], getFooterPosition(po.array()));
            assertEquals(testCase.snd[0] - testCase.snd[1], getFooterLength(po.array()));
            Object o = marsh.unmarshal(po.array(), null);
            boolean eq = o.equals(testCase.fst);
            assertEquals(testCase.fst, o);
            System.out.println("Test successful for : " + o.getClass() + "\n\n");
        }
    }

}
