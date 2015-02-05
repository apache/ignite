///* @java.file.header */
//
///*  _________        _____ __________________        _____
// *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
// *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
// *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
// *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
// */
//
//package org.apache.ignite.codegen;
//
//import com.sun.istack.internal.*;
//import org.apache.ignite.internal.*;
//import org.apache.ignite.internal.processors.cache.*;
//import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
//import org.apache.ignite.internal.processors.cache.version.*;
//import org.apache.ignite.internal.processors.clock.*;
//import org.apache.ignite.internal.util.*;
//import org.apache.ignite.internal.util.direct.*;
//import org.apache.ignite.internal.util.typedef.internal.*;
//import org.apache.ignite.lang.*;
//
//import java.io.*;
//import java.lang.reflect.*;
//import java.net.*;
//import java.nio.*;
//import java.util.*;
//
//import static java.lang.reflect.Modifier.*;
//
///**
// * Direct marshallable code generator.
// */
//public class CommunicationMessageCodeGenerator {
//    /** */
//    private static final Comparator<Field> FIELD_CMP = new Comparator<Field>() {
//        @Override public int compare(Field f1, Field f2) {
//            int ver1 = 0;
//
//            GridDirectVersion verAnn1 = f1.getAnnotation(GridDirectVersion.class);
//
//            if (verAnn1 != null) {
//                ver1 = verAnn1.value();
//
//                assert ver1 > 0;
//            }
//
//            int ver2 = 0;
//
//            GridDirectVersion verAnn2 = f2.getAnnotation(GridDirectVersion.class);
//
//            if (verAnn2 != null) {
//                ver2 = verAnn2.value();
//
//                assert ver2 > 0;
//            }
//
//            return ver1 < ver2 ? -1 : ver1 > ver2 ? 1 : f1.getName().compareTo(f2.getName());
//        }
//    };
//
//    /** */
//    private static final String SRC_DIR = U.getGridGainHome() + "/modules/core/src/main/java";
//
//    /** */
//    private static final Class<?> BASE_CLS = GridTcpCommunicationMessageAdapter.class;
//
//    /** */
//    private static final String EMPTY = "";
//
//    /** */
//    private static final String TAB = "    ";
//
//    /** */
//    private static final String COMM_STATE_VAR = "commState";
//
//    /** */
//    private static final String BUF_VAR = "buf";
//
//    /** */
//    private static final String STATE_VAR = COMM_STATE_VAR + "." + "idx";
//
//    /** */
//    private static final String TYPE_WRITTEN_VAR = COMM_STATE_VAR + "." + "typeWritten";
//
//    /** */
//    private static final String IT_VAR = COMM_STATE_VAR + "." + "it";
//
//    /** */
//    private static final String CUR_VAR = COMM_STATE_VAR + "." + "cur";
//
//    /** */
//    private static final String KEY_DONE_VAR = COMM_STATE_VAR + "." + "keyDone";
//
//    /** */
//    private static final String READ_SIZE_VAR = COMM_STATE_VAR + "." + "readSize";
//
//    /** */
//    private static final String READ_ITEMS_VAR = COMM_STATE_VAR + "." + "readItems";
//
//    /** */
//    private static final String DFLT_LOC_VAR = "_val";
//
//    /** */
//    private final Collection<String> write = new ArrayList<>();
//
//    /** */
//    private final Collection<String> read = new ArrayList<>();
//
//    /** */
//    private final Collection<String> clone = new ArrayList<>();
//
//    /** */
//    private final Collection<String> clone0 = new ArrayList<>();
//
//    /** */
//    private final Map<Class<?>, Integer> fieldCnt = new HashMap<>();
//
//    /** */
//    private List<Field> fields;
//
//    /** */
//    private int indent;
//
//    /**
//     * @param args Arguments.
//     */
//    public static void main(String[] args) {
//        CommunicationMessageCodeGenerator gen = new CommunicationMessageCodeGenerator();
//
//        try {
//            gen.generateAll(true);
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * Generates code for all classes.
//     *
//     * @param write Whether to write to file.
//     * @throws Exception In case of error.
//     */
//    public void generateAll(boolean write) throws Exception {
//        Collection<Class<? extends GridTcpCommunicationMessageAdapter>> classes = classes();
//
//        byte type = 0;
//
//        for (Class<? extends GridTcpCommunicationMessageAdapter> cls : classes) {
//            boolean isAbstract = Modifier.isAbstract(cls.getModifiers());
//
//            System.out.println("Processing class: " + cls.getName() + (isAbstract ? " (abstract)" : ""));
//
//            if (write)
//                generateAndWrite(cls, isAbstract ? -1 : type++);
//            else
//                generate(cls);
//        }
//
////        type = 0;
////
////        for (Class<? extends GridTcpCommunicationMessageAdapter> cls : classes) {
////            if (Modifier.isAbstract(cls.getModifiers()))
////                continue;
////
////            System.out.println("case " + type++ + ":");
////            System.out.println("    return new " + cls.getSimpleName() + "();");
////            System.out.println();
////        }
//    }
//
//    /**
//     * Generates code for provided class and writes it to source file.
//     * Note: this method must be called only from {@code generateAll(boolean)}
//     * and only with updating {@code CLASSES_ORDER_FILE} and other auto generated files.
//     *
//     * @param cls Class.
//     * @throws Exception In case of error.
//     */
//    @SuppressWarnings("ConstantConditions")
//    private void generateAndWrite(Class<? extends GridTcpCommunicationMessageAdapter> cls, byte type) throws Exception {
//        assert cls != null;
//
//        generate(cls);
//
//        File file = new File(SRC_DIR, cls.getName().replace('.', File.separatorChar) + ".java");
//
//        if (!file.exists() || !file.isFile()) {
//            System.out.println("    Source file not found: " + file.getPath());
//
//            return;
//        }
//
//        Collection<String> src = new ArrayList<>();
//
//        BufferedReader rdr = null;
//
//        try {
//            rdr = new BufferedReader(new FileReader(file));
//
//            String line;
//            boolean skip = false;
//
//            boolean writeFound = false;
//            boolean readFound = false;
//            boolean cloneFound = false;
//            boolean clone0Found = false;
//
//            while ((line = rdr.readLine()) != null) {
//                if (!skip) {
//                    src.add(line);
//
//                    if (line.contains("public boolean writeTo(ByteBuffer buf)")) {
//                        src.addAll(write);
//
//                        skip = true;
//
//                        writeFound = true;
//                    }
//                    else if (line.contains("public boolean readFrom(ByteBuffer buf)")) {
//                        src.addAll(read);
//
//                        skip = true;
//
//                        readFound = true;
//                    }
////                    else if (type >= 0 && line.contains("public byte directType()")) {
////                        src.add(TAB + TAB + "return " + type + ';');
////
////                        skip = true;
////                    }
//                    else if (line.contains("public GridTcpCommunicationMessageAdapter clone()")) {
//                        src.addAll(clone);
//
//                        skip = true;
//
//                        cloneFound = true;
//                    }
//                    else if (line.contains("protected void clone0(GridTcpCommunicationMessageAdapter _msg)")) {
//                        src.addAll(clone0);
//
//                        skip = true;
//
//                        clone0Found = true;
//                    }
//                }
//                else if (line.startsWith(TAB + "}")) {
//                    src.add(line);
//
//                    skip = false;
//                }
//            }
//
//            if (!writeFound)
//                System.out.println("    writeTo method doesn't exist.");
//
//            if (!readFound)
//                System.out.println("    readFrom method doesn't exist.");
//
//            if (!cloneFound)
//                System.out.println("    clone method doesn't exist.");
//
//            if (!clone0Found)
//                System.out.println("    clone0 method doesn't exist.");
//        }
//        finally {
//            if (rdr != null)
//                rdr.close();
//        }
//
//        BufferedWriter wr = null;
//
//        try {
//            wr = new BufferedWriter(new FileWriter(file));
//
//            for (String line : src)
//                wr.write(line + '\n');
//        }
//        finally {
//            if (wr != null)
//                wr.close();
//        }
//    }
//
//    /**
//     * Generates code for provided class.
//     *
//     * @param cls Class.
//     * @throws Exception In case of error.
//     */
//    public void generate(Class<? extends GridTcpCommunicationMessageAdapter> cls) throws Exception {
//        assert cls != null;
//
//        write.clear();
//        read.clear();
//        clone.clear();
//        clone0.clear();
//
//        fields = new ArrayList<>();
//
//        Field[] declaredFields = cls.getDeclaredFields();
//
//        for (Field field : declaredFields) {
//            int mod = field.getModifiers();
//
//            if (!isStatic(mod) && !isTransient(mod) && !field.isAnnotationPresent(GridDirectTransient.class))
//                fields.add(field);
//        }
//
//        Collections.sort(fields, FIELD_CMP);
//
//        indent = 2;
//
//        boolean hasSuper = cls.getSuperclass() != BASE_CLS;
//
//        String clsName = cls.getSimpleName();
//
//        if (!Modifier.isAbstract(cls.getModifiers())) {
//            clone.add(builder().a(clsName).a(" _clone = new ").a(clsName).a("();").toString());
//            clone.add(EMPTY);
//            clone.add(builder().a("clone0(_clone);").toString());
//            clone.add(EMPTY);
//            clone.add(builder().a("return _clone;").toString());
//        }
//
//        if (hasSuper) {
//            clone0.add(builder().a("super.clone0(_msg);").toString());
//            clone0.add(EMPTY);
//        }
//
//        Collection<Field> cloningFields = new ArrayList<>(declaredFields.length);
//
//        for (Field field: declaredFields)
//            if (!isStatic(field.getModifiers()))
//                cloningFields.add(field);
//
//        if (!cloningFields.isEmpty()) {
//            clone0.add(builder().a(clsName).a(" _clone = (").a(clsName).a(")_msg;").toString());
//            clone0.add(EMPTY);
//
//            for (Field field : cloningFields) {
//                String name = field.getName();
//                Class<?> type = field.getType();
//
//                String res = name;
//
//                if (BASE_CLS.isAssignableFrom(type))
//                    res = name + " != null ? (" + type.getSimpleName() + ")" + name + ".clone() : null";
//
//                clone0.add(builder().a("_clone.").a(name).a(" = ").a(res).a(";").toString());
//            }
//        }
//
//        start(write, hasSuper ? "writeTo" : null, true);
//        start(read, hasSuper ? "readFrom" : null, false);
//
//        indent++;
//
//        int state = startState(cls);
//
//        for (Field field : fields)
//            processField(field, state++);
//
//        indent--;
//
//        finish(write);
//        finish(read);
//    }
//
//    /**
//     * @param cls Message class.
//     * @return Start state.
//     */
//    private int startState(Class<?> cls) {
//        assert cls != null;
//
//        Class<?> superCls = cls.getSuperclass();
//
//        Integer state = fieldCnt.get(superCls);
//
//        if (state != null)
//            return state;
//
//        state = 0;
//
//        while (cls.getSuperclass() != BASE_CLS) {
//            cls = cls.getSuperclass();
//
//            for (Field field : cls.getDeclaredFields()) {
//                int mod = field.getModifiers();
//
//                if (!isStatic(mod) && !isTransient(mod) && !field.isAnnotationPresent(GridDirectTransient.class))
//                    state++;
//            }
//        }
//
//        fieldCnt.put(superCls, state);
//
//        return state;
//    }
//
//    /**
//     * @param code Code lines.
//     * @param superMtd Super class method name.
//     * @param writeType Whether to write message type.
//     */
//    private void start(Collection<String> code, @Nullable String superMtd, boolean writeType) {
//        assert code != null;
//
//        code.add(builder().a(COMM_STATE_VAR).a(".setBuffer(").a(BUF_VAR).a(");").toString());
//        code.add(EMPTY);
//
//        if (superMtd != null) {
//            returnFalseIfFailed(code, "super." + superMtd, BUF_VAR);
//
//            code.add(EMPTY);
//        }
//
//        if (writeType) {
//            code.add(builder().a("if (!").a(TYPE_WRITTEN_VAR).a(") {").toString());
//
//            indent++;
//
//            returnFalseIfFailed(code, COMM_STATE_VAR + ".putByte", "null", "directType()");
//
//            code.add(EMPTY);
//            code.add(builder().a(TYPE_WRITTEN_VAR).a(" = true;").toString());
//
//            indent--;
//
//            code.add(builder().a("}").toString());
//            code.add(EMPTY);
//        }
//
//        if (!fields.isEmpty())
//            code.add(builder().a("switch (").a(STATE_VAR).a(") {").toString());
//    }
//
//    /**
//     * @param code Code lines.
//     */
//    private void finish(Collection<String> code) {
//        assert code != null;
//
//        if (!fields.isEmpty()) {
//            code.add(builder().a("}").toString());
//            code.add(EMPTY);
//        }
//
//        code.add(builder().a("return true;").toString());
//    }
//
//    /**
//     * @param field Field.
//     * @param opt Case option.
//     */
//    private void processField(Field field, int opt) {
//        assert field != null;
//        assert opt >= 0;
//
//        GridDirectCollection colAnn = field.getAnnotation(GridDirectCollection.class);
//        GridDirectMap mapAnn = field.getAnnotation(GridDirectMap.class);
//
//        if (colAnn == null && Collection.class.isAssignableFrom(field.getType()))
//            throw new IllegalStateException("@GridDirectCollection annotation is not provided for field: " +
//                field.getName());
//
//        if (mapAnn == null && Map.class.isAssignableFrom(field.getType()))
//            throw new IllegalStateException("@GridDirectMap annotation is not provided for field: " + field.getName());
//
//        writeField(field, opt, colAnn, mapAnn);
//        readField(field, opt, colAnn, mapAnn);
//    }
//
//    /**
//     * @param field Field.
//     * @param opt Case option.
//     * @param colAnn Collection annotation.
//     * @param mapAnn Map annotation.
//     */
//    private void writeField(Field field, int opt, @Nullable GridDirectCollection colAnn,
//        @Nullable GridDirectMap mapAnn) {
//        assert field != null;
//        assert opt >= 0;
//
//        write.add(builder().a("case ").a(opt).a(":").toString());
//
//        indent++;
//
//        returnFalseIfWriteFailed(field.getType(), field.getName(), colAnn != null ? colAnn.value() : null,
//            mapAnn != null ? mapAnn.keyType() : null, mapAnn != null ? mapAnn.valueType() : null, false);
//
//        write.add(EMPTY);
//        write.add(builder().a(STATE_VAR).a("++;").toString());
//        write.add(EMPTY);
//
//        indent--;
//    }
//
//    /**
//     * @param field Field.
//     * @param opt Case option.
//     * @param colAnn Collection annotation.
//     * @param mapAnn Map annotation.
//     */
//    private void readField(Field field, int opt, @Nullable GridDirectCollection colAnn,
//        @Nullable GridDirectMap mapAnn) {
//        assert field != null;
//        assert opt >= 0;
//
//        read.add(builder().a("case ").a(opt).a(":").toString());
//
//        indent++;
//
//        returnFalseIfReadFailed(field.getType(), field.getName(), colAnn != null ? colAnn.value() : null,
//            mapAnn != null ? mapAnn.keyType() : null, mapAnn != null ? mapAnn.valueType() : null, false);
//
//        read.add(EMPTY);
//        read.add(builder().a(STATE_VAR).a("++;").toString());
//        read.add(EMPTY);
//
//        indent--;
//    }
//
//    /**
//     * @param type Field type.
//     * @param name Field name.
//     * @param colItemType Collection item type.
//     * @param mapKeyType Map key type.
//     * @param mapValType Map key value.
//     * @param raw Raw write flag.
//     */
//    private void returnFalseIfWriteFailed(Class<?> type, String name, @Nullable Class<?> colItemType,
//        @Nullable Class<?> mapKeyType, @Nullable Class<?> mapValType, boolean raw) {
//        assert type != null;
//        assert name != null;
//
//        String field = raw ? "null" : '"' + name + '"';
//
//        if (type == byte.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putByte", field, name);
//        else if (type == short.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putShort", field, name);
//        else if (type == int.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putInt", field, name);
//        else if (type == long.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putLong", field, name);
//        else if (type == float.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putFloat", field, name);
//        else if (type == double.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putDouble", field, name);
//        else if (type == char.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putChar", field, name);
//        else if (type == boolean.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putBoolean", field, name);
//        else if (type == byte[].class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putByteArray", field, name);
//        else if (type == short[].class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putShortArray", field, name);
//        else if (type == int[].class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putIntArray", field, name);
//        else if (type == long[].class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putLongArray", field, name);
//        else if (type == float[].class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putFloatArray", field, name);
//        else if (type == double[].class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putDoubleArray", field, name);
//        else if (type == char[].class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putCharArray", field, name);
//        else if (type == boolean[].class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putBooleanArray", field, name);
//        else if (type == UUID.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putUuid", field, name);
//        else if (type == ByteBuffer.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putByteBuffer", field, name);
//        else if (type == IgniteUuid.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putGridUuid", field, name);
//        else if (type == GridClockDeltaVersion.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putClockDeltaVersion", field, name);
//        else if (type == GridByteArrayList.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putByteArrayList", field, name);
//        else if (type == GridLongList.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putLongList", field, name);
//        else if (type == GridCacheVersion.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putCacheVersion", field, name);
//        else if (type == GridDhtPartitionExchangeId.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putDhtPartitionExchangeId", field, name);
//        else if (type == GridCacheValueBytes.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putValueBytes", field, name);
//        //else if (type == GridDrInternalRequestEntry.class)
//        //    returnFalseIfFailed(write, COMM_STATE_VAR + ".putDrInternalRequestEntry", field, name);
//        else if (type == String.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putString", field, name);
//        else if (type == BitSet.class)
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putBitSet", field, name);
//        else if (type.isEnum())
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putEnum", field, name);
//        else if (BASE_CLS.isAssignableFrom(type))
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putMessage", field, name);
//        else if (type.isArray() || Collection.class.isAssignableFrom(type)) {
//            boolean isArr = type.isArray();
//
//            if (isArr)
//                colItemType = type.getComponentType();
//
//            assert colItemType != null;
//
//            write.add(builder().a("if (").a(name).a(" != null) {").toString());
//
//            indent++;
//
//            write.add(builder().a("if (").a(IT_VAR).a(" == null) {").toString());
//
//            indent++;
//
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putInt", "null", name + "." + (isArr ? "length" : "size()"));
//
//            write.add(EMPTY);
//            write.add(builder().a(IT_VAR).a(" = ").a(isArr ? "arrayIterator(" + name + ")" : name + ".iterator()").
//                a(";").toString());
//
//            indent--;
//
//            write.add(builder().a("}").toString());
//            write.add(EMPTY);
//            write.add(builder().a("while (").a(IT_VAR).a(".hasNext() || ").a(CUR_VAR).a(" != NULL").a(") {").
//                toString());
//
//            indent++;
//
//            write.add(builder().a("if (").a(CUR_VAR).a(" == NULL)").toString());
//
//            indent++;
//
//            write.add(builder().a(CUR_VAR).a(" = ").a(IT_VAR).a(".next();").toString());
//
//            indent--;
//
//            write.add(EMPTY);
//
//            returnFalseIfWriteFailed(colItemType, "(" + colItemType.getSimpleName() + ")" + CUR_VAR,
//                null, null, null, true);
//
//            write.add(EMPTY);
//            write.add(builder().a(CUR_VAR).a(" = NULL;").toString());
//
//            indent--;
//
//            write.add(builder().a("}").toString());
//            write.add(EMPTY);
//            write.add(builder().a(IT_VAR).a(" = null;").toString());
//
//            indent--;
//
//            write.add(builder().a("} else {").toString());
//
//            indent++;
//
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putInt", "null", "-1");
//
//            indent--;
//
//            write.add(builder().a("}").toString());
//        }
//        else if (Map.class.isAssignableFrom(type)) {
//            assert mapKeyType != null;
//            assert mapValType != null;
//
//            write.add(builder().a("if (").a(name).a(" != null) {").toString());
//
//            indent++;
//
//            write.add(builder().a("if (").a(IT_VAR).a(" == null) {").toString());
//
//            indent++;
//
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putInt", "null", name + ".size()");
//
//            write.add(EMPTY);
//            write.add(builder().a(IT_VAR).a(" = ").a(name).a(".entrySet().iterator();").toString());
//
//            indent--;
//
//            write.add(builder().a("}").toString());
//            write.add(EMPTY);
//            write.add(builder().a("while (").a(IT_VAR).a(".hasNext() || ").a(CUR_VAR).a(" != NULL").a(") {").
//                toString());
//
//            indent++;
//
//            write.add(builder().a("if (").a(CUR_VAR).a(" == NULL)").toString());
//
//            indent++;
//
//            write.add(builder().a(CUR_VAR).a(" = ").a(IT_VAR).a(".next();").toString());
//
//            indent--;
//
//            String entryType = "Map.Entry<" + U.box(mapKeyType).getSimpleName() + ", " +
//                U.box(mapValType).getSimpleName() + ">";
//
//            write.add(EMPTY);
//            write.add(builder().a(entryType).a(" e = (").a(entryType).a(")").a(CUR_VAR).a(";").toString());
//            write.add(EMPTY);
//            write.add(builder().a("if (!").a(KEY_DONE_VAR).a(") {").toString());
//
//            indent++;
//
//            returnFalseIfWriteFailed(mapKeyType, "e.getKey()", null, null, null, true);
//
//            write.add(EMPTY);
//            write.add(builder().a(KEY_DONE_VAR).a(" = true;").toString());
//
//            indent--;
//
//            write.add(builder().a("}").toString());
//            write.add(EMPTY);
//
//            returnFalseIfWriteFailed(mapValType, "e.getValue()", null, null, null, true);
//
//            write.add(EMPTY);
//            write.add(builder().a(KEY_DONE_VAR).a(" = false;").toString());
//            write.add(EMPTY);
//            write.add(builder().a(CUR_VAR).a(" = NULL;").toString());
//
//            indent--;
//
//            write.add(builder().a("}").toString());
//            write.add(EMPTY);
//            write.add(builder().a(IT_VAR).a(" = null;").toString());
//
//            indent--;
//
//            write.add(builder().a("} else {").toString());
//
//            indent++;
//
//            returnFalseIfFailed(write, COMM_STATE_VAR + ".putInt", "null", "-1");
//
//            indent--;
//
//            write.add(builder().a("}").toString());
//        }
//        else
//            throw new IllegalStateException("Unsupported type: " + type);
//    }
//
//    /**
//     * @param type Field type.
//     * @param name Field name.
//     * @param colItemType Collection item type.
//     * @param mapKeyType Map key type.
//     * @param mapValType Map value type.
//     * @param raw Raw read flag.
//     */
//    private void returnFalseIfReadFailed(Class<?> type, @Nullable String name, @Nullable Class<?> colItemType,
//        @Nullable Class<?> mapKeyType, @Nullable Class<?> mapValType, boolean raw) {
//        assert type != null;
//
//        String field = raw ? "null" : '"' + name + '"';
//
//        String retType = type.getSimpleName();
//
//        if (type == byte.class)
//            returnFalseIfReadFailed("byte", name, null, COMM_STATE_VAR + ".getByte", field, false);
//        else if (type == short.class)
//            returnFalseIfReadFailed("short", name, null, COMM_STATE_VAR + ".getShort", field, false);
//        else if (type == int.class)
//            returnFalseIfReadFailed("int", name, null, COMM_STATE_VAR + ".getInt", field, false);
//        else if (type == long.class)
//            returnFalseIfReadFailed("long", name, null, COMM_STATE_VAR + ".getLong", field, false);
//        else if (type == float.class)
//            returnFalseIfReadFailed("float", name, null, COMM_STATE_VAR + ".getFloat", field, false);
//        else if (type == double.class)
//            returnFalseIfReadFailed("double", name, null, COMM_STATE_VAR + ".getDouble", field, false);
//        else if (type == char.class)
//            returnFalseIfReadFailed("char", name, null, COMM_STATE_VAR + ".getChar", field, false);
//        else if (type == boolean.class)
//            returnFalseIfReadFailed("boolean", name, null, COMM_STATE_VAR + ".getBoolean", field, false);
//        else if (type == byte[].class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getByteArray", field, false);
//        else if (type == short[].class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getShortArray", field, false);
//        else if (type == int[].class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getIntArray", field, false);
//        else if (type == long[].class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getLongArray", field, false);
//        else if (type == float[].class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getFloatArray", field, false);
//        else if (type == double[].class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getDoubleArray", field, false);
//        else if (type == char[].class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getCharArray", field, false);
//        else if (type == boolean[].class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getBooleanArray", field, false);
//        else if (type == UUID.class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getUuid", field, false);
//        else if (type == ByteBuffer.class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getByteBuffer", field, false);
//        else if (type == IgniteUuid.class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getGridUuid", field, false);
//        else if (type == GridClockDeltaVersion.class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getClockDeltaVersion", field, false);
//        else if (type == GridLongList.class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getLongList", field, false);
//        else if (type == GridByteArrayList.class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getByteArrayList", field, false);
//        else if (type == GridCacheVersion.class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getCacheVersion", field, false);
//        else if (type == GridDhtPartitionExchangeId.class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getDhtPartitionExchangeId", field, false);
//        else if (type == GridCacheValueBytes.class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getValueBytes", field, false);
//        //else if (type == GridDrInternalRequestEntry.class)
//        //    returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getDrInternalRequestEntry", field, false);
//        else if (type == String.class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getString", field, false);
//        else if (type == BitSet.class)
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getBitSet", field, false);
//        else if (type.isEnum()) {
//            assert name != null;
//
//            returnFalseIfReadFailed("byte", null, name + "0", COMM_STATE_VAR + ".getByte", field, false);
//
//            read.add(EMPTY);
//            read.add(builder().a(name).a(" = ").a(type.getSimpleName()).a(".fromOrdinal(").a(name).a("0);").toString());
//        }
//        else if (BASE_CLS.isAssignableFrom(type))
//            returnFalseIfReadFailed(retType, name, null, COMM_STATE_VAR + ".getMessage", field, true);
//        else if (type.isArray() || Collection.class.isAssignableFrom(type)) {
//            assert name != null;
//
//            boolean isArr = type.isArray();
//
//            if (isArr)
//                colItemType = type.getComponentType();
//
//            assert colItemType != null;
//
//            Class<?> colType = Set.class.isAssignableFrom(type) ? HashSet.class : ArrayList.class;
//
//            read.add(builder().a("if (").a(READ_SIZE_VAR).a(" == -1) {").toString());
//
//            indent++;
//
//            returnFalseIfReadFailed(int.class, READ_SIZE_VAR, null, null, null, true);
//
//            indent--;
//
//            read.add(builder().a("}").toString());
//            read.add(EMPTY);
//            read.add(builder().a("if (").a(READ_SIZE_VAR).a(" >= 0) {").toString());
//
//            indent++;
//
//            read.add(builder().a("if (").a(name).a(" == null)").toString());
//
//            indent++;
//
//            if (isArr) {
//                String val = colItemType.isArray() ?
//                    colItemType.getComponentType().getSimpleName() + "[" + READ_SIZE_VAR + "][]" :
//                    colItemType.getSimpleName() + "[" + READ_SIZE_VAR + "]";
//
//                read.add(builder().a(name).a(" = new ").a(val).a(";").
//                    toString());
//            }
//            else {
//                read.add(builder().a(name).a(" = new ").a(colType.getSimpleName()).a("<>(").a(READ_SIZE_VAR).a(");").
//                    toString());
//            }
//
//            indent--;
//
//            read.add(EMPTY);
//            read.add(builder().a("for (int i = ").a(READ_ITEMS_VAR).a("; i < ").a(READ_SIZE_VAR).a("; i++) {").
//                toString());
//
//            indent++;
//
//            String var = colItemType.isPrimitive() ? colItemType.getSimpleName() + " " + DFLT_LOC_VAR : null;
//
//            returnFalseIfReadFailed(colItemType, var, null, null, null, true);
//
//            read.add(EMPTY);
//            read.add(builder().a(name).a(isArr ? "[i] = " : ".add(").a("(").a(U.box(colItemType).getSimpleName()).
//                a(")").a(DFLT_LOC_VAR).a(isArr ? ";" : ");").toString());
//            read.add(EMPTY);
//            read.add(builder().a(READ_ITEMS_VAR).a("++;").toString());
//
//            indent--;
//
//            read.add(builder().a("}").toString());
//
//            indent--;
//
//            read.add(builder().a("}").toString());
//            read.add(EMPTY);
//            read.add(builder().a(READ_SIZE_VAR).a(" = -1;").toString());
//            read.add(builder().a(READ_ITEMS_VAR).a(" = 0;").toString());
//        }
//        else if (Map.class.isAssignableFrom(type)) {
//            assert name != null;
//            assert mapKeyType != null;
//            assert mapValType != null;
//
//            Class<?> mapType = LinkedHashMap.class.isAssignableFrom(type) ? LinkedHashMap.class : HashMap.class;
//
//            read.add(builder().a("if (").a(READ_SIZE_VAR).a(" == -1) {").toString());
//
//            indent++;
//
//            returnFalseIfReadFailed(int.class, READ_SIZE_VAR, null, null, null, true);
//
//            indent--;
//
//            read.add(builder().a("}").toString());
//            read.add(EMPTY);
//            read.add(builder().a("if (").a(READ_SIZE_VAR).a(" >= 0) {").toString());
//
//            indent++;
//
//            read.add(builder().a("if (").a(name).a(" == null)").toString());
//
//            indent++;
//
//            read.add(builder().a(name).a(" = new ").a(mapType.getSimpleName()).a("<>(").a(READ_SIZE_VAR).a(", 1.0f);").
//                toString());
//
//            indent--;
//
//            read.add(EMPTY);
//            read.add(builder().a("for (int i = ").a(READ_ITEMS_VAR).a("; i < ").a(READ_SIZE_VAR).a("; i++) {").
//                toString());
//
//            indent++;
//
//            read.add(builder().a("if (!").a(KEY_DONE_VAR).a(") {").toString());
//
//            indent++;
//
//            String var = mapKeyType.isPrimitive() ? mapKeyType.getSimpleName() + " " + DFLT_LOC_VAR : null;
//
//            returnFalseIfReadFailed(mapKeyType, var, null, null, null, true);
//
//            read.add(EMPTY);
//            read.add(builder().a(CUR_VAR).a(" = ").a(DFLT_LOC_VAR).a(";").toString());
//            read.add(builder().a(KEY_DONE_VAR).a(" = true;").toString());
//
//            indent--;
//
//            read.add(builder().a("}").toString());
//            read.add(EMPTY);
//
//            var = mapValType.isPrimitive() ? mapValType.getSimpleName() + " " + DFLT_LOC_VAR : null;
//
//            returnFalseIfReadFailed(mapValType, var, null, null, null, true);
//
//            read.add(EMPTY);
//            read.add(builder().a(name).a(".put((").a(U.box(mapKeyType).getSimpleName()).a(")").a(CUR_VAR).
//                a(", ").a(DFLT_LOC_VAR).a(");").toString());
//            read.add(EMPTY);
//            read.add(builder().a(KEY_DONE_VAR).a(" = false;").toString());
//            read.add(EMPTY);
//            read.add(builder().a(READ_ITEMS_VAR).a("++;").toString());
//
//            indent--;
//
//            read.add(builder().a("}").toString());
//
//            indent--;
//
//            read.add(builder().a("}").toString());
//            read.add(EMPTY);
//            read.add(builder().a(READ_SIZE_VAR).a(" = -1;").toString());
//            read.add(builder().a(READ_ITEMS_VAR).a(" = 0;").toString());
//            read.add(builder().a(CUR_VAR).a(" = null;").toString());
//        }
//        else
//            throw new IllegalStateException("Unsupported type: " + type);
//    }
//
//    /**
//     * @param retType Return type.
//     * @param var Variable name.
//     * @param locVar Local variable name.
//     * @param mtd Method name.
//     * @param arg Method argument.
//     * @param cast Whether cast is needed.
//     */
//    private void returnFalseIfReadFailed(String retType, @Nullable String var, @Nullable String locVar, String mtd,
//        @Nullable String arg, boolean cast) {
//        assert retType != null;
//        assert mtd != null;
//
//        if (var == null)
//            var = retType + " " + (locVar != null ? locVar : DFLT_LOC_VAR);
//
//        read.add(builder().a(var).a(" = ").a(cast ? "(" + retType + ")" : "").a(mtd).
//            a(arg != null ? "(" + arg + ");" : "();").toString());
//        read.add(EMPTY);
//
//        read.add(builder().a("if (!").a(COMM_STATE_VAR).a(".lastRead())").toString());
//
//        indent++;
//
//        read.add(builder().a("return false;").toString());
//
//        indent--;
//    }
//
//    /**
//     * @param code Code lines.
//     * @param accessor Field or method name.
//     * @param args Method arguments.
//     */
//    private void returnFalseIfFailed(Collection<String> code, String accessor, @Nullable String... args) {
//        assert code != null;
//        assert accessor != null;
//
//        String argsStr = "";
//
//        if (args != null && args.length > 0) {
//            for (String arg : args)
//                argsStr += arg + ", ";
//
//            argsStr = argsStr.substring(0, argsStr.length() - 2);
//        }
//
//        code.add(builder().a("if (!").a(accessor).a("(" + argsStr + ")").a(")").toString());
//
//        indent++;
//
//        code.add(builder().a("return false;").toString());
//
//        indent--;
//    }
//
//    /**
//     * Creates new builder with correct indent.
//     *
//     * @return Builder.
//     */
//    private SB builder() {
//        assert indent > 0;
//
//        SB sb = new SB();
//
//        for (int i = 0; i < indent; i++)
//            sb.a(TAB);
//
//        return sb;
//    }
//
//    /**
//     * Gets all direct marshallable classes.
//     * First classes will be classes from {@code classesOrder} with same order
//     * as ordered values. Other classes will be at the end and ordered by name
//     * (with package prefix).
//     * That orders need for saving {@code directType} value.
//     *
//     * @return Classes.
//     * @throws Exception In case of error.
//     */
//    private Collection<Class<? extends GridTcpCommunicationMessageAdapter>> classes() throws Exception {
//        Collection<Class<? extends GridTcpCommunicationMessageAdapter>> col = new TreeSet<>(
//            new Comparator<Class<? extends GridTcpCommunicationMessageAdapter>>() {
//                @Override public int compare(Class<? extends GridTcpCommunicationMessageAdapter> c1,
//                    Class<? extends GridTcpCommunicationMessageAdapter> c2) {
//                    return c1.getName().compareTo(c2.getName());
//                }
//            });
//
//        URLClassLoader ldr = (URLClassLoader)getClass().getClassLoader();
//
//        for (URL url : ldr.getURLs()) {
//            File file = new File(url.toURI());
//
//            int prefixLen = file.getPath().length() + 1;
//
//            processFile(file, ldr, prefixLen, col);
//        }
//
//        return col;
//    }
//
//    /**
//     * Recursively process provided file or directory.
//     *
//     * @param file File.
//     * @param ldr Class loader.
//     * @param prefixLen Path prefix length.
//     * @param col Classes.
//     * @throws Exception In case of error.
//     */
//    private void processFile(File file, ClassLoader ldr, int prefixLen,
//        Collection<Class<? extends GridTcpCommunicationMessageAdapter>> col) throws Exception {
//        assert file != null;
//        assert ldr != null;
//        assert prefixLen > 0;
//        assert col != null;
//
//        if (!file.exists())
//            throw new FileNotFoundException("File doesn't exist: " + file);
//
//        if (file.isDirectory()) {
//            for (File f : file.listFiles())
//                processFile(f, ldr, prefixLen, col);
//        }
//        else {
//            assert file.isFile();
//
//            String path = file.getPath();
//
//            if (path.endsWith(".class")) {
//                String clsName = path.substring(prefixLen, path.length() - 6).replace(File.separatorChar, '.');
//
//                Class<?> cls = Class.forName(clsName, false, ldr);
//
//                if (cls.getDeclaringClass() == null && cls.getEnclosingClass() == null &&
//                    !BASE_CLS.equals(cls) && BASE_CLS.isAssignableFrom(cls))
//                    col.add((Class<? extends GridTcpCommunicationMessageAdapter>)cls);
//            }
//        }
//    }
//}
