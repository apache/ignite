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

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.util.*;

import static java.lang.reflect.Modifier.*;

/**
* Direct marshallable code generator.
*/
public class CommunicationMessageCodeGenerator {
    /** */
    private static final Comparator<Field> FIELD_CMP = new Comparator<Field>() {
        @Override public int compare(Field f1, Field f2) {
            return f1.getName().compareTo(f2.getName());
        }
    };

    /** */
    private static final String[] EXCLUDED_PACKAGES = new String[] {
        "org.apache.ignite.internal.processors.rest.client.message",
        "org.apache.ignite.internal.processors.rest.protocols.tcp"
    };

    /** */
    private static final String SRC_DIR = U.getIgniteHome() + "/modules/core/src/main/java";

    /** */
    private static final Class<?> BASE_CLS = MessageAdapter.class;

    /** */
    private static final String EMPTY = "";

    /** */
    private static final String TAB = "    ";

    /** */
    private static final String BUF_VAR = "buf";

    /** */
    private static final Map<Class<?>, MessageAdapter.Type> TYPES = U.newHashMap(30);

    static {
        TYPES.put(byte.class, MessageAdapter.Type.BYTE);
        TYPES.put(Byte.class, MessageAdapter.Type.BYTE);
        TYPES.put(short.class, MessageAdapter.Type.SHORT);
        TYPES.put(Short.class, MessageAdapter.Type.SHORT);
        TYPES.put(int.class, MessageAdapter.Type.INT);
        TYPES.put(Integer.class, MessageAdapter.Type.INT);
        TYPES.put(long.class, MessageAdapter.Type.LONG);
        TYPES.put(Long.class, MessageAdapter.Type.LONG);
        TYPES.put(float.class, MessageAdapter.Type.FLOAT);
        TYPES.put(Float.class, MessageAdapter.Type.FLOAT);
        TYPES.put(double.class, MessageAdapter.Type.DOUBLE);
        TYPES.put(Double.class, MessageAdapter.Type.DOUBLE);
        TYPES.put(char.class, MessageAdapter.Type.CHAR);
        TYPES.put(Character.class, MessageAdapter.Type.CHAR);
        TYPES.put(boolean.class, MessageAdapter.Type.BOOLEAN);
        TYPES.put(Boolean.class, MessageAdapter.Type.BOOLEAN);
        TYPES.put(byte[].class, MessageAdapter.Type.BYTE_ARR);
        TYPES.put(short[].class, MessageAdapter.Type.SHORT_ARR);
        TYPES.put(int[].class, MessageAdapter.Type.INT_ARR);
        TYPES.put(long[].class, MessageAdapter.Type.LONG_ARR);
        TYPES.put(float[].class, MessageAdapter.Type.FLOAT_ARR);
        TYPES.put(double[].class, MessageAdapter.Type.DOUBLE_ARR);
        TYPES.put(char[].class, MessageAdapter.Type.CHAR_ARR);
        TYPES.put(boolean[].class, MessageAdapter.Type.BOOLEAN_ARR);
        TYPES.put(String.class, MessageAdapter.Type.STRING);
        TYPES.put(BitSet.class, MessageAdapter.Type.BIT_SET);
        TYPES.put(UUID.class, MessageAdapter.Type.UUID);
        TYPES.put(IgniteUuid.class, MessageAdapter.Type.IGNITE_UUID);
    }

    /**
     * @param cls Class.
     * @return Type enum value.
     */
    private static MessageAdapter.Type typeEnum(Class<?> cls) {
        MessageAdapter.Type type = TYPES.get(cls);

        if (type == null) {
            assert MessageAdapter.class.isAssignableFrom(cls) : cls;

            type = MessageAdapter.Type.MSG;
        }

        return type;
    }

    /** */
    private final Collection<String> write = new ArrayList<>();

    /** */
    private final Collection<String> read = new ArrayList<>();

    /** */
    private final Collection<String> clone = new ArrayList<>();

    /** */
    private final Collection<String> clone0 = new ArrayList<>();

    /** */
    private final Map<Class<?>, Integer> fieldCnt = new HashMap<>();

    /** */
    private List<Field> fields;

    /** */
    private int indent;

    /**
     * @param args Arguments.
     */
    public static void main(String[] args) {
        CommunicationMessageCodeGenerator gen = new CommunicationMessageCodeGenerator();

        try {
            gen.generateAll(true);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Generates code for all classes.
     *
     * @param write Whether to write to file.
     * @throws Exception In case of error.
     */
    public void generateAll(boolean write) throws Exception {
        Collection<Class<? extends MessageAdapter>> classes = classes();

        for (Class<? extends MessageAdapter> cls : classes) {
            boolean isAbstract = Modifier.isAbstract(cls.getModifiers());

            System.out.println("Processing class: " + cls.getName() + (isAbstract ? " (abstract)" : ""));

            if (write)
                generateAndWrite(cls);
            else
                generate(cls);
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
    private void generateAndWrite(Class<? extends MessageAdapter> cls) throws Exception {
        assert cls != null;

        generate(cls);

        File file = new File(SRC_DIR, cls.getName().replace('.', File.separatorChar) + ".java");

        if (!file.exists() || !file.isFile()) {
            System.out.println("    Source file not found: " + file.getPath());

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
            boolean cloneFound = false;
            boolean clone0Found = false;

            while ((line = rdr.readLine()) != null) {
                if (!skip) {
                    src.add(line);

                    if (line.contains("public boolean writeTo(ByteBuffer buf, MessageWriteState state)")) {
                        src.addAll(write);

                        skip = true;

                        writeFound = true;
                    }
                    else if (line.contains("public boolean readFrom(ByteBuffer buf)")) {
                        src.addAll(read);

                        skip = true;

                        readFound = true;
                    }
                    else if (line.contains("public MessageAdapter clone()")) {
                        src.addAll(clone);

                        skip = true;

                        cloneFound = true;
                    }
                    else if (line.contains("protected void clone0(MessageAdapter _msg)")) {
                        src.addAll(clone0);

                        skip = true;

                        clone0Found = true;
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

            if (!cloneFound)
                System.out.println("    clone method doesn't exist.");

            if (!clone0Found)
                System.out.println("    clone0 method doesn't exist.");
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
    public void generate(Class<? extends MessageAdapter> cls) throws Exception {
        assert cls != null;

        write.clear();
        read.clear();
        clone.clear();
        clone0.clear();

        fields = new ArrayList<>();

        Field[] declaredFields = cls.getDeclaredFields();

        for (Field field : declaredFields) {
            int mod = field.getModifiers();

            if (!isStatic(mod) && !isTransient(mod) && !field.isAnnotationPresent(GridDirectTransient.class))
                fields.add(field);
        }

        Collections.sort(fields, FIELD_CMP);

        indent = 2;

        boolean hasSuper = cls.getSuperclass() != BASE_CLS;

        String clsName = cls.getSimpleName();

        if (!Modifier.isAbstract(cls.getModifiers())) {
            clone.add(builder().a("throw new UnsupportedOperationException();").toString());

//            clone.add(builder().a(clsName).a(" _clone = new ").a(clsName).a("();").toString());
//            clone.add(EMPTY);
//            clone.add(builder().a("clone0(_clone);").toString());
//            clone.add(EMPTY);
//            clone.add(builder().a("return _clone;").toString());
        }

        if (hasSuper) {
            clone0.add(builder().a("super.clone0(_msg);").toString());
            clone0.add(EMPTY);
        }

        Collection<Field> cloningFields = new ArrayList<>(declaredFields.length);

        for (Field field: declaredFields)
            if (!isStatic(field.getModifiers()))
                cloningFields.add(field);

        if (!cloningFields.isEmpty()) {
            clone0.add(builder().a(clsName).a(" _clone = (").a(clsName).a(")_msg;").toString());
            clone0.add(EMPTY);

            for (Field field : cloningFields) {
                String name = field.getName();
                Class<?> type = field.getType();

                String res = name;

                if (BASE_CLS.isAssignableFrom(type))
                    res = name + " != null ? (" + type.getSimpleName() + ")" + name + ".clone() : null";

                clone0.add(builder().a("_clone.").a(name).a(" = ").a(res).a(";").toString());
            }
        }

        start(write, hasSuper ? "writeTo" : null, true);
        start(read, hasSuper ? "readFrom" : null, false);

        indent++;

        int state = startState(cls);

        for (Field field : fields)
            processField(field, state++);

        indent--;

        finish(write);
        finish(read);
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

        while (cls.getSuperclass() != BASE_CLS) {
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

        if (write) {
            code.add(builder().a("MessageWriter writer = state.writer();").toString());
            code.add(EMPTY);
        }

        code.add(builder().a(write ? "writer" : "reader").a(".setBuffer(").a(BUF_VAR).a(");").toString());
        code.add(EMPTY);

        if (superMtd != null) {
            if (write)
                returnFalseIfFailed(code, "super." + superMtd, BUF_VAR, "state");
            else
                returnFalseIfFailed(code, "super." + superMtd, BUF_VAR);

            code.add(EMPTY);
        }

        if (write) {
            code.add(builder().a("if (!state.isTypeWritten()) {").toString());

            indent++;

            returnFalseIfFailed(code, "writer.writeByte", "null", "directType()");

            code.add(EMPTY);
            code.add(builder().a("state.setTypeWritten();").toString());

            indent--;

            code.add(builder().a("}").toString());
            code.add(EMPTY);
        }

        if (!fields.isEmpty())
            code.add(builder().a("switch (").a(write ? "state.index()" : "readState").a(") {").toString());
    }

    /**
     * @param code Code lines.
     */
    private void finish(Collection<String> code) {
        assert code != null;

        if (!fields.isEmpty()) {
            code.add(builder().a("}").toString());
            code.add(EMPTY);
        }

        code.add(builder().a("return true;").toString());
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

        returnFalseIfWriteFailed(field.getType(), field.getName(), colAnn != null ? colAnn.value() : null,
            mapAnn != null ? mapAnn.keyType() : null, mapAnn != null ? mapAnn.valueType() : null, false);

        write.add(EMPTY);
        write.add(builder().a("state.increment();").toString());
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

        returnFalseIfReadFailed(field.getType(), field.getName(), colAnn != null ? colAnn.value() : null,
            mapAnn != null ? mapAnn.keyType() : null, mapAnn != null ? mapAnn.valueType() : null);

        read.add(EMPTY);
        read.add(builder().a("readState++;").toString());
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
        @Nullable Class<?> mapKeyType, @Nullable Class<?> mapValType, boolean raw) {
        assert type != null;
        assert name != null;

        String field = raw ? "null" : '"' + name + '"';

        if (type == byte.class)
            returnFalseIfFailed(write, "writer.writeByte", field, name);
        else if (type == short.class)
            returnFalseIfFailed(write, "writer.writeShort", field, name);
        else if (type == int.class)
            returnFalseIfFailed(write, "writer.writeInt", field, name);
        else if (type == long.class)
            returnFalseIfFailed(write, "writer.writeLong", field, name);
        else if (type == float.class)
            returnFalseIfFailed(write, "writer.writeFloat", field, name);
        else if (type == double.class)
            returnFalseIfFailed(write, "writer.writeDouble", field, name);
        else if (type == char.class)
            returnFalseIfFailed(write, "writer.writeChar", field, name);
        else if (type == boolean.class)
            returnFalseIfFailed(write, "writer.writeBoolean", field, name);
        else if (type == byte[].class)
            returnFalseIfFailed(write, "writer.writeByteArray", field, name);
        else if (type == short[].class)
            returnFalseIfFailed(write, "writer.writeShortArray", field, name);
        else if (type == int[].class)
            returnFalseIfFailed(write, "writer.writeIntArray", field, name);
        else if (type == long[].class)
            returnFalseIfFailed(write, "writer.writeLongArray", field, name);
        else if (type == float[].class)
            returnFalseIfFailed(write, "writer.writeFloatArray", field, name);
        else if (type == double[].class)
            returnFalseIfFailed(write, "writer.writeDoubleArray", field, name);
        else if (type == char[].class)
            returnFalseIfFailed(write, "writer.writeCharArray", field, name);
        else if (type == boolean[].class)
            returnFalseIfFailed(write, "writer.writeBooleanArray", field, name);
        else if (type == String.class)
            returnFalseIfFailed(write, "writer.writeString", field, name);
        else if (type == BitSet.class)
            returnFalseIfFailed(write, "writer.writeBitSet", field, name);
        else if (type == UUID.class)
            returnFalseIfFailed(write, "writer.writeUuid", field, name);
        else if (type == IgniteUuid.class)
            returnFalseIfFailed(write, "writer.writeIgniteUuid", field, name);
        else if (type.isEnum()) {
            String arg = name + " != null ? (byte)" + name + ".ordinal() : -1";

            returnFalseIfFailed(write, "writer.writeByte", field, arg);
        }
        else if (BASE_CLS.isAssignableFrom(type))
            returnFalseIfFailed(write, "writer.writeMessage", field, name);
        else if (type.isArray()) {
            returnFalseIfFailed(write, "writer.writeObjectArray", field, name,
                "Type." + typeEnum(type.getComponentType()));
        }
        else if (Collection.class.isAssignableFrom(type) && !Set.class.isAssignableFrom(type)) {
            assert colItemType != null;

            returnFalseIfFailed(write, "writer.writeCollection", field, name, "Type." + typeEnum(colItemType));
        }
        else if (Map.class.isAssignableFrom(type)) {
            assert mapKeyType != null;
            assert mapValType != null;

            returnFalseIfFailed(write, "writer.writeMap", field, name, "Type." + typeEnum(mapKeyType),
                "Type." + typeEnum(mapValType));
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
        @Nullable Class<?> mapKeyType, @Nullable Class<?> mapValType) {
        assert type != null;

        String field = '"' + name + '"';

        if (type == byte.class)
            returnFalseIfReadFailed(name, "reader.readByte", field);
        else if (type == short.class)
            returnFalseIfReadFailed(name, "reader.readShort", field);
        else if (type == int.class)
            returnFalseIfReadFailed(name, "reader.readInt", field);
        else if (type == long.class)
            returnFalseIfReadFailed(name, "reader.readLong", field);
        else if (type == float.class)
            returnFalseIfReadFailed(name, "reader.readFloat", field);
        else if (type == double.class)
            returnFalseIfReadFailed(name, "reader.readDouble", field);
        else if (type == char.class)
            returnFalseIfReadFailed(name, "reader.readChar", field);
        else if (type == boolean.class)
            returnFalseIfReadFailed(name, "reader.readBoolean", field);
        else if (type == byte[].class)
            returnFalseIfReadFailed(name, "reader.readByteArray", field);
        else if (type == short[].class)
            returnFalseIfReadFailed(name, "reader.readShortArray", field);
        else if (type == int[].class)
            returnFalseIfReadFailed(name, "reader.readIntArray", field);
        else if (type == long[].class)
            returnFalseIfReadFailed(name, "reader.readLongArray", field);
        else if (type == float[].class)
            returnFalseIfReadFailed(name, "reader.readFloatArray", field);
        else if (type == double[].class)
            returnFalseIfReadFailed(name, "reader.readDoubleArray", field);
        else if (type == char[].class)
            returnFalseIfReadFailed(name, "reader.readCharArray", field);
        else if (type == boolean[].class)
            returnFalseIfReadFailed(name, "reader.readBooleanArray", field);
        else if (type == String.class)
            returnFalseIfReadFailed(name, "reader.readString", field);
        else if (type == BitSet.class)
            returnFalseIfReadFailed(name, "reader.readBitSet", field);
        else if (type == UUID.class)
            returnFalseIfReadFailed(name, "reader.readUuid", field);
        else if (type == IgniteUuid.class)
            returnFalseIfReadFailed(name, "reader.readIgniteUuid", field);
        else if (type.isEnum()) {
            String loc = name + "Ord";

            read.add(builder().a("byte ").a(loc).a(";").toString());
            read.add(EMPTY);

            returnFalseIfReadFailed(loc, "reader.readByte", field);

            read.add(EMPTY);
            read.add(builder().a(name).a(" = ").a(type.getSimpleName()).a(".fromOrdinal(").a(loc).a(");").toString());
        }
        else if (BASE_CLS.isAssignableFrom(type))
            returnFalseIfReadFailed(name, "reader.readMessage", field);
        else if (type.isArray())
            returnFalseIfReadFailed(name, "reader.readObjectArray", field, "Type." + typeEnum(type.getComponentType()));
        else if (Collection.class.isAssignableFrom(type) && !Set.class.isAssignableFrom(type)) {
            assert colItemType != null;

            returnFalseIfReadFailed(name, "reader.readCollection", field, "Type." + typeEnum(colItemType));
        }
        else if (Map.class.isAssignableFrom(type)) {
            assert mapKeyType != null;
            assert mapValType != null;

            boolean linked = type.equals(LinkedHashMap.class);

            returnFalseIfReadFailed(name, "reader.readMap", field, "Type." + typeEnum(mapKeyType),
                "Type." + typeEnum(mapValType), linked ? "true" : "false");
        }
        else
            throw new IllegalStateException("Unsupported type: " + type);
    }

    /**
     * @param var Variable name.
     * @param mtd Method name.
     * @param args Method arguments.
     */
    private void returnFalseIfReadFailed(String var, String mtd, @Nullable String... args) {
        assert mtd != null;

        String argsStr = "";

        if (args != null && args.length > 0) {
            for (String arg : args)
                argsStr += arg + ", ";

            argsStr = argsStr.substring(0, argsStr.length() - 2);
        }

        read.add(builder().a(var).a(" = ").a(mtd).a("(").a(argsStr).a(");").toString());
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
    private Collection<Class<? extends MessageAdapter>> classes() throws Exception {
        Collection<Class<? extends MessageAdapter>> col = new TreeSet<>(
            new Comparator<Class<? extends MessageAdapter>>() {
                @Override public int compare(Class<? extends MessageAdapter> c1,
                    Class<? extends MessageAdapter> c2) {
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
    private void processFile(File file, ClassLoader ldr, int prefixLen,
        Collection<Class<? extends MessageAdapter>> col) throws Exception {
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

                for (String excluded : EXCLUDED_PACKAGES) {
                    if (clsName.startsWith(excluded))
                        return;
                }

                Class<?> cls = Class.forName(clsName, false, ldr);

                if (cls.getDeclaringClass() == null && cls.getEnclosingClass() == null &&
                    !BASE_CLS.equals(cls) && BASE_CLS.isAssignableFrom(cls))
                    col.add((Class<? extends MessageAdapter>)cls);
            }
        }
    }
}
