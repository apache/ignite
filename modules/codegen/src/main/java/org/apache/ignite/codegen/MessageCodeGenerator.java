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

package org.apache.ignite.codegen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.internal.GridCodegenConverter;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.jetbrains.annotations.Nullable;

import static java.lang.reflect.Modifier.isStatic;
import static java.lang.reflect.Modifier.isTransient;

/**
* Direct marshallable code generator.
*/
public class MessageCodeGenerator {
    /** */
    private static final Comparator<Field> FIELD_CMP = new Comparator<Field>() {
        @Override public int compare(Field f1, Field f2) {
            return f1.getName().compareTo(f2.getName());
        }
    };

    /** */
    private static final String DFLT_SRC_DIR = U.getIgniteHome() + "/modules/core/src/main/java";

    /** */
    private static final String INDEXING_SRC_DIR = U.getIgniteHome() + "/modules/indexing/src/main/java";

    /** */
    private static final Class<?> BASE_CLS = Message.class;

    /** */
    private static final String EMPTY = "";

    /** */
    private static final String TAB = "    ";

    /** */
    private static final String BUF_VAR = "buf";

    /** */
    private static final Map<Class<?>, MessageCollectionItemType> TYPES = U.newHashMap(30);

    static {
        TYPES.put(byte.class, MessageCollectionItemType.BYTE);
        TYPES.put(Byte.class, MessageCollectionItemType.BYTE);
        TYPES.put(short.class, MessageCollectionItemType.SHORT);
        TYPES.put(Short.class, MessageCollectionItemType.SHORT);
        TYPES.put(int.class, MessageCollectionItemType.INT);
        TYPES.put(Integer.class, MessageCollectionItemType.INT);
        TYPES.put(long.class, MessageCollectionItemType.LONG);
        TYPES.put(Long.class, MessageCollectionItemType.LONG);
        TYPES.put(float.class, MessageCollectionItemType.FLOAT);
        TYPES.put(Float.class, MessageCollectionItemType.FLOAT);
        TYPES.put(double.class, MessageCollectionItemType.DOUBLE);
        TYPES.put(Double.class, MessageCollectionItemType.DOUBLE);
        TYPES.put(char.class, MessageCollectionItemType.CHAR);
        TYPES.put(Character.class, MessageCollectionItemType.CHAR);
        TYPES.put(boolean.class, MessageCollectionItemType.BOOLEAN);
        TYPES.put(Boolean.class, MessageCollectionItemType.BOOLEAN);
        TYPES.put(byte[].class, MessageCollectionItemType.BYTE_ARR);
        TYPES.put(short[].class, MessageCollectionItemType.SHORT_ARR);
        TYPES.put(int[].class, MessageCollectionItemType.INT_ARR);
        TYPES.put(long[].class, MessageCollectionItemType.LONG_ARR);
        TYPES.put(float[].class, MessageCollectionItemType.FLOAT_ARR);
        TYPES.put(double[].class, MessageCollectionItemType.DOUBLE_ARR);
        TYPES.put(char[].class, MessageCollectionItemType.CHAR_ARR);
        TYPES.put(boolean[].class, MessageCollectionItemType.BOOLEAN_ARR);
        TYPES.put(String.class, MessageCollectionItemType.STRING);
        TYPES.put(BitSet.class, MessageCollectionItemType.BIT_SET);
        TYPES.put(UUID.class, MessageCollectionItemType.UUID);
        TYPES.put(IgniteUuid.class, MessageCollectionItemType.IGNITE_UUID);
    }

    /**
     * @param cls Class.
     * @return Type enum value.
     */
    private static MessageCollectionItemType typeEnum(Class<?> cls) {
        MessageCollectionItemType type = TYPES.get(cls);

        if (type == null) {
            assert Message.class.isAssignableFrom(cls) : cls;

            type = MessageCollectionItemType.MSG;
        }

        return type;
    }

    /** */
    private final Collection<String> write = new ArrayList<>();

    /** */
    private final Collection<String> read = new ArrayList<>();

    /** */
    private final Map<Class<?>, Integer> fieldCnt = new HashMap<>();

    /** */
    private final String srcDir;

    /** */
    private int totalFieldCnt;

    /** */
    private List<Field> fields;

    /** */
    private int indent;

    /**
     * @param args Arguments.
     * @throws Exception In case of error.
     */
    public static void main(String[] args) throws Exception {
        String srcDir = DFLT_SRC_DIR;

        if (args != null && args.length > 0)
            srcDir = args[0];

        MessageCodeGenerator gen = new MessageCodeGenerator(srcDir);

//        gen.generateAll(true);

//        gen.generateAndWrite(GridNearAtomicUpdateRequest.class);

//        gen.generateAndWrite(GridMessageCollection.class);
//        gen.generateAndWrite(DataStreamerEntry.class);

//        gen.generateAndWrite(GridDistributedLockRequest.class);
//        gen.generateAndWrite(GridDistributedLockResponse.class);
//        gen.generateAndWrite(GridNearLockRequest.class);
//        gen.generateAndWrite(GridNearLockResponse.class);
//        gen.generateAndWrite(GridDhtLockRequest.class);
//        gen.generateAndWrite(GridDhtLockResponse.class);
//
//        gen.generateAndWrite(GridDistributedTxPrepareRequest.class);
//        gen.generateAndWrite(GridDistributedTxPrepareResponse.class);
//        gen.generateAndWrite(GridNearTxPrepareRequest.class);
//        gen.generateAndWrite(GridNearTxPrepareResponse.class);
//        gen.generateAndWrite(GridDhtTxPrepareRequest.class);
//        gen.generateAndWrite(GridDhtTxPrepareResponse.class);
//
//        gen.generateAndWrite(GridDistributedTxFinishRequest.class);
//        gen.generateAndWrite(GridDistributedTxFinishResponse.class);
//        gen.generateAndWrite(GridNearTxFinishRequest.class);
//        gen.generateAndWrite(GridNearTxFinishResponse.class);
//        gen.generateAndWrite(GridDhtTxFinishRequest.class);
//        gen.generateAndWrite(GridDhtTxFinishResponse.class);
//
//        gen.generateAndWrite(GridCacheTxRecoveryRequest.class);
//        gen.generateAndWrite(GridCacheTxRecoveryResponse.class);

//        gen.generateAndWrite(GridQueryCancelRequest.class);
//        gen.generateAndWrite(GridQueryFailResponse.class);
//        gen.generateAndWrite(GridQueryNextPageRequest.class);
//        gen.generateAndWrite(GridQueryNextPageResponse.class);
//        gen.generateAndWrite(GridQueryRequest.class);
//        gen.generateAndWrite(GridCacheSqlQuery.class);

//        gen.generateAndWrite(GridH2Null.class);
//        gen.generateAndWrite(GridH2Boolean.class);
//        gen.generateAndWrite(GridH2Byte.class);
//        gen.generateAndWrite(GridH2Short.class);
//        gen.generateAndWrite(GridH2Integer.class);
//        gen.generateAndWrite(GridH2Long.class);
//        gen.generateAndWrite(GridH2Decimal.class);
//        gen.generateAndWrite(GridH2Double.class);
//        gen.generateAndWrite(GridH2Float.class);
//        gen.generateAndWrite(GridH2Time.class);
//        gen.generateAndWrite(GridH2Date.class);
//        gen.generateAndWrite(GridH2Timestamp.class);
//        gen.generateAndWrite(GridH2Bytes.class);
//        gen.generateAndWrite(GridH2String.class);
//        gen.generateAndWrite(GridH2Array.class);
//        gen.generateAndWrite(GridH2JavaObject.class);
//        gen.generateAndWrite(GridH2Uuid.class);
//        gen.generateAndWrite(GridH2Geometry.class);
//        gen.generateAndWrite(GridH2CacheObject.class);
//        gen.generateAndWrite(GridH2IndexRangeRequest.class);
//        gen.generateAndWrite(GridH2IndexRangeResponse.class);
//        gen.generateAndWrite(GridH2RowRange.class);
//        gen.generateAndWrite(GridH2RowRangeBounds.class);
//        gen.generateAndWrite(GridH2QueryRequest.class);
//        gen.generateAndWrite(GridH2RowMessage.class);
    }

    /**
     * @param srcDir Source directory.
     */
    public MessageCodeGenerator(String srcDir) {
        this.srcDir = srcDir;
    }

    /**
     * Generates code for all classes.
     *
     * @param write Whether to write to file.
     * @throws Exception In case of error.
     */
    public void generateAll(boolean write) throws Exception {
        Collection<Class<? extends Message>> classes = classes();

        for (Class<? extends Message> cls : classes) {
            try {
                boolean isAbstract = Modifier.isAbstract(cls.getModifiers());

                System.out.println("Processing class: " + cls.getName() + (isAbstract ? " (abstract)" : ""));

                if (write)
                    generateAndWrite(cls);
                else
                    generate(cls);
            }
            catch (IllegalStateException e) {
                System.out.println("Will skip class generation [cls=" + cls + ", err=" + e.getMessage() + ']');
            }
        }
    }

    /**
     * Generates code for provided class and writes it to source file.
     * Note: this method must be called only from {@code generateAll(boolean)}
     * and only with updating {@code CLASSES_ORDER_FILE} and other auto generated files.
     *
     * @param cls Class.
     * @throws Exception In case of error.
     */
    @SuppressWarnings("ConstantConditions")
    public void generateAndWrite(Class<? extends Message> cls) throws Exception {
        assert cls != null;

        generate(cls);

        File file = new File(srcDir, cls.getName().replace('.', File.separatorChar) + ".java");

        if (!file.exists() || !file.isFile()) {
            System.out.println("Source file not found: " + file.getPath());

            return;
        }

        Collection<String> src = new ArrayList<>();

        BufferedReader rdr = null;

        try {
            rdr = new BufferedReader(new FileReader(file));

            String line;
            boolean skip = false;

            boolean writeFound = false;
            boolean readFound = false;
            boolean fieldCntFound = false;

            while ((line = rdr.readLine()) != null) {
                if (!skip) {
                    src.add(line);

                    if (line.contains("public boolean writeTo(ByteBuffer buf, MessageWriter writer)")) {
                        src.addAll(write);

                        skip = true;

                        writeFound = true;
                    }
                    else if (line.contains("public boolean readFrom(ByteBuffer buf, MessageReader reader)")) {
                        src.addAll(read);

                        skip = true;

                        readFound = true;
                    }
                    else if (line.contains("public byte fieldsCount()")) {
                        src.add(TAB + TAB + "return " + totalFieldCnt + ";");

                        skip = true;

                        fieldCntFound = true;
                    }
                }
                else if (line.startsWith(TAB + "}")) {
                    src.add(line);

                    skip = false;
                }
            }

            if (!writeFound)
                System.out.println("    writeTo method doesn't exist.");

            if (!readFound)
                System.out.println("    readFrom method doesn't exist.");

            if (!fieldCntFound)
                System.out.println("    fieldCount method doesn't exist.");
        }
        finally {
            if (rdr != null)
                rdr.close();
        }

        BufferedWriter wr = null;

        try {
            wr = new BufferedWriter(new FileWriter(file));

            for (String line : src)
                wr.write(line + '\n');
        }
        finally {
            if (wr != null)
                wr.close();
        }
    }

    /**
     * Generates code for provided class.
     *
     * @param cls Class.
     * @throws Exception In case of error.
     */
    private void generate(Class<? extends Message> cls) throws Exception {
        assert cls != null;

        if (cls.isInterface())
            return;

        if (cls.isAnnotationPresent(IgniteCodeGeneratingFail.class))
            throw new IllegalStateException("@IgniteCodeGeneratingFail is provided for class: " + cls.getName());

        write.clear();
        read.clear();

        fields = new ArrayList<>();

        Field[] declaredFields = cls.getDeclaredFields();

        for (Field field : declaredFields) {
            int mod = field.getModifiers();

            if (!isStatic(mod) && !isTransient(mod) && !field.isAnnotationPresent(GridDirectTransient.class))
                fields.add(field);
        }

        Collections.sort(fields, FIELD_CMP);

        int state = startState(cls);

        totalFieldCnt = state + fields.size();

        indent = 2;

        boolean hasSuper = cls.getSuperclass() != Object.class;

        start(write, hasSuper ? "writeTo" : null, true);
        start(read, hasSuper ? "readFrom" : null, false);

        indent++;

        for (Field field : fields)
            processField(field, state++);

        indent--;

        finish(write, null);
        finish(read, cls.getSimpleName());
    }

    /**
     * @param cls Message class.
     * @return Start state.
     */
    private int startState(Class<?> cls) {
        assert cls != null;

        Class<?> superCls = cls.getSuperclass();

        Integer state = fieldCnt.get(superCls);

        if (state != null)
            return state;

        state = 0;

        while (cls.getSuperclass() != Object.class) {
            cls = cls.getSuperclass();

            for (Field field : cls.getDeclaredFields()) {
                int mod = field.getModifiers();

                if (!isStatic(mod) && !isTransient(mod) && !field.isAnnotationPresent(GridDirectTransient.class))
                    state++;
            }
        }

        fieldCnt.put(superCls, state);

        return state;
    }

    /**
     * @param code Code lines.
     * @param superMtd Super class method name.
     * @param write Whether write code is generated.
     */
    private void start(Collection<String> code, @Nullable String superMtd, boolean write) {
        assert code != null;

        code.add(builder().a(write ? "writer" : "reader").a(".setBuffer(").a(BUF_VAR).a(");").toString());
        code.add(EMPTY);

        if (!write) {
            code.add(builder().a("if (!reader.beforeMessageRead())").toString());

            indent++;

            code.add(builder().a("return false;").toString());
            code.add(EMPTY);

            indent--;
        }

        if (superMtd != null) {
            if (write)
                returnFalseIfFailed(code, "super." + superMtd, BUF_VAR, "writer");
            else
                returnFalseIfFailed(code, "super." + superMtd, BUF_VAR, "reader");

            code.add(EMPTY);
        }

        if (write) {
            code.add(builder().a("if (!writer.isHeaderWritten()) {").toString());

            indent++;

            returnFalseIfFailed(code, "writer.writeHeader", "directType()", "fieldsCount()");

            code.add(EMPTY);
            code.add(builder().a("writer.onHeaderWritten();").toString());

            indent--;

            code.add(builder().a("}").toString());
            code.add(EMPTY);
        }

        if (!fields.isEmpty())
            code.add(builder().a("switch (").a(write ? "writer.state()" : "reader.state()").a(") {").toString());
    }

    /**
     * @param code Code lines.
     */
    private void finish(Collection<String> code, String readClsName) {
        assert code != null;

        if (!fields.isEmpty()) {
            code.add(builder().a("}").toString());
            code.add(EMPTY);
        }

        if (readClsName == null)
            code.add(builder().a("return true;").toString());
        else
            code.add(builder().a("return reader.afterMessageRead(").a(readClsName).a(".class);").toString());
    }

    /**
     * @param field Field.
     * @param opt Case option.
     */
    private void processField(Field field, int opt) {
        assert field != null;
        assert opt >= 0;

        GridDirectCollection colAnn = field.getAnnotation(GridDirectCollection.class);
        GridDirectMap mapAnn = field.getAnnotation(GridDirectMap.class);

        if (colAnn == null && Collection.class.isAssignableFrom(field.getType()))
            throw new IllegalStateException("@GridDirectCollection annotation is not provided for field: " +
                field.getName());

        if (mapAnn == null && Map.class.isAssignableFrom(field.getType()))
            throw new IllegalStateException("@GridDirectMap annotation is not provided for field: " + field.getName());

        writeField(field, opt, colAnn, mapAnn);
        readField(field, opt, colAnn, mapAnn);
    }

    /**
     * @param field Field.
     * @param opt Case option.
     * @param colAnn Collection annotation.
     * @param mapAnn Map annotation.
     */
    private void writeField(Field field, int opt, @Nullable GridDirectCollection colAnn,
        @Nullable GridDirectMap mapAnn) {
        assert field != null;
        assert opt >= 0;

        write.add(builder().a("case ").a(opt).a(":").toString());

        indent++;

        GridCodegenConverter fldPreproc = field.getAnnotation(GridCodegenConverter.class);

        String getExp = (fldPreproc != null && !fldPreproc.get().isEmpty())? fldPreproc.get(): field.getName();
        Class<?> writeType = (fldPreproc != null && !fldPreproc.type().equals(GridCodegenConverter.Default.class))?
            fldPreproc.type(): field.getType();

        returnFalseIfWriteFailed(writeType, field.getName(), colAnn != null ? colAnn.value() : null,
            mapAnn != null ? mapAnn.keyType() : null, mapAnn != null ? mapAnn.valueType() : null, false, getExp);

        write.add(EMPTY);
        write.add(builder().a("writer.incrementState();").toString());
        write.add(EMPTY);

        indent--;
    }

    /**
     * @param field Field.
     * @param opt Case option.
     * @param colAnn Collection annotation.
     * @param mapAnn Map annotation.
     */
    private void readField(Field field, int opt, @Nullable GridDirectCollection colAnn,
        @Nullable GridDirectMap mapAnn) {
        assert field != null;
        assert opt >= 0;

        read.add(builder().a("case ").a(opt).a(":").toString());

        indent++;

        GridCodegenConverter fldPreproc = field.getAnnotation(GridCodegenConverter.class);
        String setExp = (fldPreproc != null && !fldPreproc.get().isEmpty())? fldPreproc.set(): "";
        Class<?> writeType = (fldPreproc != null && !fldPreproc.type().equals(GridCodegenConverter.Default.class))?
            fldPreproc.type(): field.getType();

        returnFalseIfReadFailed(writeType, field.getName(), colAnn != null ? colAnn.value() : null,
            mapAnn != null ? mapAnn.keyType() : null, mapAnn != null ? mapAnn.valueType() : null, setExp);

        read.add(EMPTY);
        read.add(builder().a("reader.incrementState();").toString());
        read.add(EMPTY);

        indent--;
    }

    /**
     * @param type Field type.
     * @param name Field name.
     * @param colItemType Collection item type.
     * @param mapKeyType Map key type.
     * @param mapValType Map key value.
     * @param raw Raw write flag.
     */
    private void returnFalseIfWriteFailed(Class<?> type, String name, @Nullable Class<?> colItemType,
        @Nullable Class<?> mapKeyType, @Nullable Class<?> mapValType, boolean raw, String getExpr) {
        assert type != null;
        assert name != null;

        String field = raw ? "null" : '"' + name + '"';

        if (type == byte.class)
            returnFalseIfFailed(write, "writer.writeByte", field, getExpr);
        else if (type == short.class)
            returnFalseIfFailed(write, "writer.writeShort", field, getExpr);
        else if (type == int.class)
            returnFalseIfFailed(write, "writer.writeInt", field, getExpr);
        else if (type == long.class)
            returnFalseIfFailed(write, "writer.writeLong", field, getExpr);
        else if (type == float.class)
            returnFalseIfFailed(write, "writer.writeFloat", field, getExpr);
        else if (type == double.class)
            returnFalseIfFailed(write, "writer.writeDouble", field, getExpr);
        else if (type == char.class)
            returnFalseIfFailed(write, "writer.writeChar", field, getExpr);
        else if (type == boolean.class)
            returnFalseIfFailed(write, "writer.writeBoolean", field, getExpr);
        else if (type == byte[].class)
            returnFalseIfFailed(write, "writer.writeByteArray", field, getExpr);
        else if (type == short[].class)
            returnFalseIfFailed(write, "writer.writeShortArray", field, getExpr);
        else if (type == int[].class)
            returnFalseIfFailed(write, "writer.writeIntArray", field, getExpr);
        else if (type == long[].class)
            returnFalseIfFailed(write, "writer.writeLongArray", field, getExpr);
        else if (type == float[].class)
            returnFalseIfFailed(write, "writer.writeFloatArray", field, getExpr);
        else if (type == double[].class)
            returnFalseIfFailed(write, "writer.writeDoubleArray", field, getExpr);
        else if (type == char[].class)
            returnFalseIfFailed(write, "writer.writeCharArray", field, getExpr);
        else if (type == boolean[].class)
            returnFalseIfFailed(write, "writer.writeBooleanArray", field, getExpr);
        else if (type == String.class)
            returnFalseIfFailed(write, "writer.writeString", field, getExpr);
        else if (type == BitSet.class)
            returnFalseIfFailed(write, "writer.writeBitSet", field, getExpr);
        else if (type == UUID.class)
            returnFalseIfFailed(write, "writer.writeUuid", field, getExpr);
        else if (type == IgniteUuid.class)
            returnFalseIfFailed(write, "writer.writeIgniteUuid", field, getExpr);
        else if (type.isEnum()) {
            String arg = getExpr + " != null ? (byte)" + getExpr + ".ordinal() : -1";

            returnFalseIfFailed(write, "writer.writeByte", field, arg);
        }
        else if (BASE_CLS.isAssignableFrom(type))
            returnFalseIfFailed(write, "writer.writeMessage", field, getExpr);
        else if (type.isArray()) {
            returnFalseIfFailed(write, "writer.writeObjectArray", field, getExpr,
                "MessageCollectionItemType." + typeEnum(type.getComponentType()));
        }
        else if (Collection.class.isAssignableFrom(type) && !Set.class.isAssignableFrom(type)) {
            assert colItemType != null;

            returnFalseIfFailed(write, "writer.writeCollection", field, getExpr,
                "MessageCollectionItemType." + typeEnum(colItemType));
        }
        else if (Map.class.isAssignableFrom(type)) {
            assert mapKeyType != null;
            assert mapValType != null;

            returnFalseIfFailed(write, "writer.writeMap", field, getExpr,
                "MessageCollectionItemType." + typeEnum(mapKeyType),
                "MessageCollectionItemType." + typeEnum(mapValType));
        }
        else
            throw new IllegalStateException("Unsupported type: " + type);
    }

    /**
     * @param type Field type.
     * @param name Field name.
     * @param colItemType Collection item type.
     * @param mapKeyType Map key type.
     * @param mapValType Map value type.
     */
    private void returnFalseIfReadFailed(Class<?> type, @Nullable String name, @Nullable Class<?> colItemType,
        @Nullable Class<?> mapKeyType, @Nullable Class<?> mapValType, String setExpr) {
        assert type != null;

        String field = '"' + name + '"';

        if (type == byte.class)
            returnFalseIfReadFailed(name, "reader.readByte", setExpr, field);
        else if (type == short.class)
            returnFalseIfReadFailed(name, "reader.readShort", setExpr, field);
        else if (type == int.class)
            returnFalseIfReadFailed(name, "reader.readInt", setExpr, field);
        else if (type == long.class)
            returnFalseIfReadFailed(name, "reader.readLong", setExpr, field);
        else if (type == float.class)
            returnFalseIfReadFailed(name, "reader.readFloat", setExpr, field);
        else if (type == double.class)
            returnFalseIfReadFailed(name, "reader.readDouble", setExpr, field);
        else if (type == char.class)
            returnFalseIfReadFailed(name, "reader.readChar", setExpr, field);
        else if (type == boolean.class)
            returnFalseIfReadFailed(name, "reader.readBoolean", setExpr, field);
        else if (type == byte[].class)
            returnFalseIfReadFailed(name, "reader.readByteArray", setExpr, field);
        else if (type == short[].class)
            returnFalseIfReadFailed(name, "reader.readShortArray", setExpr, field);
        else if (type == int[].class)
            returnFalseIfReadFailed(name, "reader.readIntArray", setExpr, field);
        else if (type == long[].class)
            returnFalseIfReadFailed(name, "reader.readLongArray", setExpr, field);
        else if (type == float[].class)
            returnFalseIfReadFailed(name, "reader.readFloatArray", setExpr, field);
        else if (type == double[].class)
            returnFalseIfReadFailed(name, "reader.readDoubleArray", setExpr, field);
        else if (type == char[].class)
            returnFalseIfReadFailed(name, "reader.readCharArray", setExpr, field);
        else if (type == boolean[].class)
            returnFalseIfReadFailed(name, "reader.readBooleanArray", setExpr, field);
        else if (type == String.class)
            returnFalseIfReadFailed(name, "reader.readString", setExpr, field);
        else if (type == BitSet.class)
            returnFalseIfReadFailed(name, "reader.readBitSet", setExpr, field);
        else if (type == UUID.class)
            returnFalseIfReadFailed(name, "reader.readUuid", setExpr, field);
        else if (type == IgniteUuid.class)
            returnFalseIfReadFailed(name, "reader.readIgniteUuid", setExpr, field);
        else if (type.isEnum()) {
            String loc = name + "Ord";

            read.add(builder().a("byte ").a(loc).a(";").toString());
            read.add(EMPTY);

            returnFalseIfReadFailed(loc, "reader.readByte", setExpr, field);

            read.add(EMPTY);
            read.add(builder().a(name).a(" = ").a(type.getSimpleName()).a(".fromOrdinal(").a(loc).a(");").toString());
        }
        else if (BASE_CLS.isAssignableFrom(type))
            returnFalseIfReadFailed(name, "reader.readMessage", setExpr, field);
        else if (type.isArray()) {
            Class<?> compType = type.getComponentType();

            returnFalseIfReadFailed(name, "reader.readObjectArray", setExpr, field,
                "MessageCollectionItemType." + typeEnum(compType),
                compType.getSimpleName() + ".class");
        }
        else if (Collection.class.isAssignableFrom(type) && !Set.class.isAssignableFrom(type)) {
            assert colItemType != null;

            returnFalseIfReadFailed(name, "reader.readCollection", setExpr, field,
                "MessageCollectionItemType." + typeEnum(colItemType));
        }
        else if (Map.class.isAssignableFrom(type)) {
            assert mapKeyType != null;
            assert mapValType != null;

            boolean linked = type.equals(LinkedHashMap.class);

            returnFalseIfReadFailed(name, "reader.readMap", setExpr, field,
                "MessageCollectionItemType." + typeEnum(mapKeyType),
                "MessageCollectionItemType." + typeEnum(mapValType),
                linked ? "true" : "false");
        }
        else
            throw new IllegalStateException("Unsupported type: " + type);
    }

    /**
     * @param var Variable name.
     * @param mtd Method name.
     * @param args Method arguments.
     */
    private void returnFalseIfReadFailed(String var, String mtd, String setConverter, @Nullable String... args) {
        assert mtd != null;

        String argsStr = "";

        if (args != null && args.length > 0) {
            for (String arg : args)
                argsStr += arg + ", ";

            argsStr = argsStr.substring(0, argsStr.length() - 2);
        }

        if (setConverter.isEmpty())
            read.add(builder().a(var).a(" = ").a(mtd).a("(").a(argsStr).a(");").toString());
        else {
            read.add(builder().a(var).a(" = ").a(setConverter
                .replace("$val$", new SB().a(mtd).a("(").a(argsStr).a(")").toString())).a(";").toString());
        }
        read.add(EMPTY);

        read.add(builder().a("if (!reader.isLastRead())").toString());

        indent++;

        read.add(builder().a("return false;").toString());

        indent--;
    }

    /**
     * @param code Code lines.
     * @param accessor Field or method name.
     * @param args Method arguments.
     */
    private void returnFalseIfFailed(Collection<String> code, String accessor, @Nullable String... args) {
        assert code != null;
        assert accessor != null;

        String argsStr = "";

        if (args != null && args.length > 0) {
            for (String arg : args)
                argsStr += arg + ", ";

            argsStr = argsStr.substring(0, argsStr.length() - 2);
        }

        code.add(builder().a("if (!").a(accessor).a("(").a(argsStr).a("))").toString());

        indent++;

        code.add(builder().a("return false;").toString());

        indent--;
    }

    /**
     * Creates new builder with correct indent.
     *
     * @return Builder.
     */
    private SB builder() {
        assert indent > 0;

        SB sb = new SB();

        for (int i = 0; i < indent; i++)
            sb.a(TAB);

        return sb;
    }

    /**
     * Gets all direct marshallable classes.
     * First classes will be classes from {@code classesOrder} with same order
     * as ordered values. Other classes will be at the end and ordered by name
     * (with package prefix).
     * That orders need for saving {@code directType} value.
     *
     * @return Classes.
     * @throws Exception In case of error.
     */
    private Collection<Class<? extends Message>> classes() throws Exception {
        Collection<Class<? extends Message>> col = new TreeSet<>(
            new Comparator<Class<? extends Message>>() {
                @Override public int compare(Class<? extends Message> c1,
                    Class<? extends Message> c2) {
                    return c1.getName().compareTo(c2.getName());
                }
            });

        URLClassLoader ldr = (URLClassLoader)getClass().getClassLoader();

        for (URL url : ldr.getURLs()) {
            File file = new File(url.toURI());

            int prefixLen = file.getPath().length() + 1;

            processFile(file, ldr, prefixLen, col);
        }

        return col;
    }

    /**
     * Recursively process provided file or directory.
     *
     * @param file File.
     * @param ldr Class loader.
     * @param prefixLen Path prefix length.
     * @param col Classes.
     * @throws Exception In case of error.
     */
    @SuppressWarnings("unchecked")
    private void processFile(File file, ClassLoader ldr, int prefixLen,
        Collection<Class<? extends Message>> col) throws Exception {
        assert file != null;
        assert ldr != null;
        assert prefixLen > 0;
        assert col != null;

        if (!file.exists())
            throw new FileNotFoundException("File doesn't exist: " + file);

        if (file.isDirectory()) {
            for (File f : file.listFiles())
                processFile(f, ldr, prefixLen, col);
        }
        else {
            assert file.isFile();

            String path = file.getPath();

            if (path.endsWith(".class")) {
                String clsName = path.substring(prefixLen, path.length() - 6).replace(File.separatorChar, '.');

                Class<?> cls = Class.forName(clsName, false, ldr);

                if (cls.getDeclaringClass() == null && cls.getEnclosingClass() == null &&
                    !BASE_CLS.equals(cls) && BASE_CLS.isAssignableFrom(cls))
                    col.add((Class<? extends Message>)cls);
            }
        }
    }
}
