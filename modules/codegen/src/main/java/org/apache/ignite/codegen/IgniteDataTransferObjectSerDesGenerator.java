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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.DeclarativeCommandAdapter;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.LocalCommand;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import static java.lang.reflect.Modifier.isStatic;
import static java.lang.reflect.Modifier.isTransient;
import static org.apache.ignite.codegen.MessageCodeGenerator.DFLT_SRC_DIR;
import static org.apache.ignite.codegen.MessageCodeGenerator.TAB;

/**
 * This class can generate boilerplate code for classes extends {@link IgniteDataTransferObject}.
 * This class can generate {@code IgniteDataTransferObject#writeExternalData(ObjectOutput)}
 * and {@code IgniteDataTransferObject#readExternalData(byte, ObjectInput)}.
 */
public class IgniteDataTransferObjectSerDesGenerator {
    /** */
    public static final String METHOD_JAVADOC = TAB + "/** {@inheritDoc} */";

    /** */
    public static final String IMPORT_TOKEN = "import ";

    /** */
    public static final String IMPORT_STATIC_TOKEN = "import static ";

    /** */
    public static final String WRITE_MTD = "writeExternalData";

    /** */
    private int methodsStart = Integer.MAX_VALUE;

    /** */
    private final Set<String> imports = new TreeSet<>();

    /** */
    private final Set<String> staticImports = new TreeSet<>();

    /** */
    private static final Map<Class<?>, GridTuple3<Function<String, String>, Function<String, String>, Boolean>> TYPE_GENS
        = new HashMap<>();

    static {
        TYPE_GENS.put(byte.class, F.t(
            fld -> "out.writeByte(" + fld + ");",
            fld -> fld + " = in.readByte();",
            false
        ));

        TYPE_GENS.put(byte[].class, F.t(
            fld -> "U.writeByteArray(out, " + fld + ");",
            fld -> fld + " = U.readByteArray(in);",
            true
        ));

        TYPE_GENS.put(short.class, F.t(
            fld -> "out.writeShort(" + fld + ");",
            fld -> fld + " = in.readShort();",
            false
        ));

        TYPE_GENS.put(short[].class, F.t(
            fld -> "U.writeShortArray(out, " + fld + ");",
            fld -> fld + " = U.readShortArray(in);",
            true
        ));

        TYPE_GENS.put(int.class, F.t(
            fld -> "out.writeInt(" + fld + ");",
            fld -> fld + " = in.readInt();",
            false
        ));

        TYPE_GENS.put(int[].class, F.t(
            fld -> "U.writeIntArray(out, " + fld + ");",
            fld -> fld + " = U.readIntArray(in);",
            true
        ));

        TYPE_GENS.put(long.class, F.t(
            fld -> "out.writeLong(" + fld + ");",
            fld -> fld + " = in.readLong();",
            false
        ));

        TYPE_GENS.put(Long.class, F.t(
            fld -> "out.writeObject(" + fld + ");",
            fld -> fld + " = (Long)in.readObject();",
            false
        ));

        TYPE_GENS.put(long[].class, F.t(
            fld -> "U.writeLongArray(out, " + fld + ");",
            fld -> fld + " = U.readLongArray(in);",
            true
        ));

        TYPE_GENS.put(float.class, F.t(
            fld -> "out.writeFloat(" + fld + ");",
            fld -> fld + " = in.readFloat();",
            false
        ));

        TYPE_GENS.put(float[].class, F.t(
            fld -> "U.writeFloatArray(out, " + fld + ");",
            fld -> fld + " = U.readFloatArray(in);",
            true
        ));

        TYPE_GENS.put(double.class, F.t(
            fld -> "out.writeDouble(" + fld + ");",
            fld -> fld + " = in.readDouble();",
            false
        ));

        TYPE_GENS.put(Double.class, F.t(
            fld -> "out.writeObject(" + fld + ");",
            fld -> fld + " = (Double)in.readObject();",
            false
        ));

        TYPE_GENS.put(double[].class, F.t(
            fld -> "U.writeDoubleArray(out, " + fld + ");",
            fld -> fld + " = U.readDoubleArray(in);",
            true
        ));

        TYPE_GENS.put(boolean.class, F.t(
            fld -> "out.writeBoolean(" + fld + ");",
            fld -> fld + " = in.readBoolean();",
            false
        ));

        TYPE_GENS.put(boolean[].class, F.t(
            fld -> "U.writeBooleanArray(out, " + fld + ");",
            fld -> fld + " = U.readBooleanArray(in);",
            true
        ));

        TYPE_GENS.put(String.class, F.t(
            fld -> "U.writeString(out, " + fld + ");",
            fld -> fld + " = U.readString(in);",
            true
        ));

        TYPE_GENS.put(UUID.class, F.t(
            fld -> "U.writeUuid(out, " + fld + ");",
            fld -> fld + " = U.readUuid(in);",
            true
        ));

        TYPE_GENS.put(IgniteUuid.class, F.t(
            fld -> "U.writeIgniteUuid(out, " + fld + ");",
            fld -> fld + " = U.readIgniteUuid(in);",
            true
        ));

        TYPE_GENS.put(Collection.class, F.t(
            fld -> "U.writeCollection(out, " + fld + ");",
            fld -> fld + " = U.readCollection(in);",
            true
        ));

        TYPE_GENS.put(List.class, F.t(
            fld -> "U.writeCollection(out, " + fld + ");",
            fld -> fld + " = U.readList(in);",
            true
        ));

        TYPE_GENS.put(Set.class, F.t(
            fld -> "U.writeCollection(out, " + fld + ");",
            fld -> fld + " = U.readSet(in);",
            true
        ));

        TYPE_GENS.put(GridCacheVersion.class, F.t(
            fld -> "out.writeObject(" + fld + ");",
            fld -> fld + " = (GridCacheVersion)in.readObject();",
            false
        ));
    }

    /** @param args Command line arguments. */
    public static void main(String[] args) {
        IgniteDataTransferObjectSerDesGenerator gen = new IgniteDataTransferObjectSerDesGenerator();

        Arrays.stream(CommandList.values())
            .filter(cmd -> cmd.command() instanceof DeclarativeCommandAdapter)
            .forEach(cmd -> gen.generate(((DeclarativeCommandAdapter<?>)cmd.command()).command()));
    }

    /** */
    private void generate(Command<?, ?> cmd) {
        boolean generate = true;

        if (cmd instanceof CommandsRegistry) {
            generate = cmd instanceof ComputeCommand || cmd instanceof LocalCommand;

            ((CommandsRegistry<?, ?>)cmd).commands().forEachRemaining(entry -> generate(entry.getValue()));
        }

        if (!generate)
            return;

        try {
            generateAndWrite(cmd.argClass(), DFLT_SRC_DIR);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private void generateAndWrite(Class<? extends IgniteDataTransferObject> cls, String srcDir) throws IOException {
        clear();

        if (cls.getName().indexOf('$') != -1) {
            System.err.println("Inner class not supported: " + cls.getName());

            return;
        }

        File file = new File(srcDir, cls.getName().replace('.', File.separatorChar) + ".java");

        if (!file.exists() || !file.isFile())
            throw new IllegalArgumentException("Source file not found: " + file.getPath());

        List<String> src =
            removeExisting(addSerialVersionUID(Files.readAllLines(file.toPath(), StandardCharsets.UTF_8)));

        List<List<String>> readWriteMethods = generateMethods(cls);
        List<List<String>> getersSeters = generateGetSet(cls);

        List<String> code = new ArrayList<>();

        int i = 0;

        // Outputs all before imports.
        for (; i < src.size(); i++) {
            code.add(src.get(i));

            if (src.get(i).startsWith("package"))
                break;
        }

        code.add("");

        i++;

        for (String imp : imports)
            code.add("import " + imp);

        for (String imp : staticImports)
            code.add("import static " + imp);

        i++;

        if (methodsStart == Integer.MAX_VALUE) // If methods not exists add them to the end.
            methodsStart = src.size() - 1;

        for (; i < methodsStart; i++)
            code.add(src.get(i));

        if (!readWriteMethods.get(0).isEmpty()) {
            code.add("");
            code.addAll(readWriteMethods.get(0));
        }

        if (!readWriteMethods.get(1).isEmpty()) {
            code.add("");
            code.addAll(readWriteMethods.get(1));
        }

        for (int j = 0; j < getersSeters.size(); j++) {
            code.add("");
            code.addAll(getersSeters.get(j));
        }

        for (; i < src.size(); i++)
            code.add(src.get(i));

        try (FileWriter writer = new FileWriter(file)) {
            for (String line : code) {
                writer.write(line);
                writer.write('\n');
            }
        }
    }

    /** */
    private List<List<String>> generateGetSet(Class<? extends IgniteDataTransferObject> cls) {
        List<Field> flds = fields(cls);

        if (flds.isEmpty())
            return Collections.emptyList();

        List<List<String>> getSet = new ArrayList<>();

        for (Field fld : flds) {
            if (!geterExists(cls, fld)) {
                List<String> geter = new ArrayList<>();

                geter.add(TAB + "/** */");
                geter.add(TAB + "public " + fld.getType().getSimpleName() + " " + fld.getName() + "() {");
                geter.add(TAB + TAB + "return " + fld.getName() + ";");
                geter.add(TAB + "}");

                getSet.add(geter);
            }

            if (!seterExists(cls, fld)) {
                List<String> seter = new ArrayList<>();

                seter.add(TAB + "/** */");
                seter.add(TAB + "public void " + fld.getName() +
                    "(" + fld.getType().getSimpleName() + " " + fld.getName() + ") {");
                seter.add(TAB + TAB + "this." + fld.getName() + " = " + fld.getName() + ";");
                seter.add(TAB + "}");

                getSet.add(seter);
            }
        }

        return getSet;
    }

    /** */
    private List<String> addSerialVersionUID(List<String> src) {
        int classStart = -1;

        for (int i = 0; i < src.size(); i++) {
            String line = src.get(i);

            if (line.contains("private static final long serialVersionUID = "))
                return src;

            if (line.startsWith("public class "))
                classStart = i;
        }

        if (classStart == -1)
            return src;

        List<String> res = new ArrayList<>(src.subList(0, classStart + 1));

        res.add(TAB + "/** */");
        res.add(TAB + "private static final long serialVersionUID = 0;");
        res.add("");
        res.addAll(src.subList(classStart + 1, src.size()));

        return res;
    }

    /** */
    private List<List<String>> generateMethods(Class<? extends IgniteDataTransferObject> cls) {
        List<Field> flds = fields(cls);

        if (flds.isEmpty() && writeExternalDataMethodExists(cls.getSuperclass()))
            return Arrays.asList(Collections.emptyList(), Collections.emptyList());

        List<String> write = new ArrayList<>();
        List<String> read = new ArrayList<>();

        write.add(METHOD_JAVADOC);
        write.add(TAB + "@Override protected void " + WRITE_MTD + "(ObjectOutput out) throws IOException {");

        read.add(METHOD_JAVADOC);
        read.add(TAB + "@Override protected void readExternalData(byte protoVer, ObjectInput in) " +
            "throws IOException, ClassNotFoundException {");

        addImport(IOException.class);
        addImport(ObjectOutput.class);
        addImport(ObjectInput.class);

        if (flds.isEmpty()) {
            write.add(TAB + TAB + "//No-op.");
            read.add(TAB + TAB + "//No-op.");
        }
        else if (writeExternalDataMethodExists(cls.getSuperclass())) {
            write.add(TAB + TAB + "super." + WRITE_MTD + "(out);");
            write.add("");

            read.add(TAB + TAB + "super.readExternalData(protoVer, in);");
            read.add("");
        }

        for (Field fld : flds) {
            String name = fld.getName();

            if (Enum.class.isAssignableFrom(fld.getType())) {
                addImport(U.class);

                write.add(TAB + TAB + "U.writeEnum(out, " + (name.equals("out") ? "this.out." : "") + name + ");");
                read.add(TAB + TAB + (name.equals("in") ? "this." : "") + name +
                    " = U.readEnum(in, " + fld.getType().getSimpleName() + ".class);");
            }
            else if (fld.getType().isArray() && !fld.getType().getComponentType().isPrimitive()) {
                addImport(U.class);
                addImport(fld.getType().getComponentType());

                write.add(TAB + TAB + "U.writeArray(out, " + (name.equals("out") ? "this.out." : "") + name + ");");
                read.add(TAB + TAB + (name.equals("in") ? "this." : "") + name +
                    " = U.readArray(in, " + fld.getType().getComponentType().getSimpleName() + ".class);");
            }
            else {
                GridTuple3<Function<String, String>, Function<String, String>, Boolean> gen
                    = TYPE_GENS.get(fld.getType());

                if (gen == null)
                    throw new IllegalArgumentException(fld.getType() + " not supported[cls=" + cls.getName() + ']');

                if (gen.get3())
                    addImport(U.class);

                write.add(TAB + TAB + gen.get1().apply((name.equals("out") ? "this." : "") + name));
                read.add(TAB + TAB + gen.get2().apply((name.equals("in") ? "this." : "") + name));
            }
        }

        write.add(TAB + "}");
        read.add(TAB + "}");

        return Arrays.asList(write, read);
    }

    /** */
    private static List<Field> fields(Class<? extends IgniteDataTransferObject> cls) {
        List<Field> flds = Arrays.stream(cls.getDeclaredFields())
            .filter(fld -> {
                int mod = fld.getModifiers();

                return !isStatic(mod) && !isTransient(mod);
            })
            .collect(Collectors.toList());
        return flds;
    }

    /** */
    private List<String> removeExisting(List<String> src) {
        return removeMethod(
            removeMethod(
                collectAndRemoveImports(src),
                WRITE_MTD
            ),
            "readExternalData"
        );
    }

    /** */
    private List<String> removeMethod(List<String> src, String methodName) {
        int start = -1;
        int finish = -1;
        int bracketCnt = -1;

        for (int i = 0; i < src.size(); i++) {
            String line = src.get(i);

            if (line.contains(methodName) && line.endsWith("{")) {
                assert src.get(i - 1).equals(METHOD_JAVADOC);

                // One line for comment and one for empty line between methods.
                start = i - 2;
                bracketCnt = 1;
            }
            else if (start != -1) {
                bracketCnt += counfOf(line, '{') - counfOf(line, '}');

                if (bracketCnt < 0)
                    throw new IllegalStateException("Wrong brackets count");

                if (bracketCnt == 0) {
                    finish = i;
                    break;
                }
            }
        }

        if (start == -1 || finish == -1)
            return src;

        methodsStart = start;

        List<String> res = new ArrayList<>(src.subList(0, start));

        res.addAll(src.subList(finish + 1, src.size()));

        return res;
    }

    /** */
    private List<String> collectAndRemoveImports(List<String> src) {
        return src.stream()
            .peek(line -> {
                if (line.startsWith(IMPORT_STATIC_TOKEN))
                    staticImports.add(line.substring(IMPORT_STATIC_TOKEN.length()));
                else if (line.startsWith(IMPORT_TOKEN))
                    imports.add(line.substring(IMPORT_TOKEN.length()));
            })
            .filter(line -> !line.startsWith(IMPORT_TOKEN))
            .collect(Collectors.toList());
    }

    /** */
    private int counfOf(String line, char ch) {
        int cnt = 0;
        int idx = line.indexOf(ch);

        while (idx != -1) {
            cnt++;
            idx = line.indexOf(ch, idx + 1);
        }

        return cnt;
    }

    /** */
    private void addImport(Class<?> cls) {
        if ("java.lang".equals(cls.getPackage().getName()))
            return;

        imports.add(cls.getName() + ';');
    }

    /** */
    private void clear() {
        methodsStart = Integer.MAX_VALUE;
        imports.clear();
        staticImports.clear();
    }

    /** */
    private static boolean geterExists(Class<? extends IgniteDataTransferObject> cls, Field fld) {
        try {
            return cls.getMethod(fld.getName()).getReturnType() == fld.getType();
        }
        catch (NoSuchMethodException e) {
            return false;
        }
    }

    /** */
    private static boolean seterExists(Class<? extends IgniteDataTransferObject> cls, Field fld) {
        try {
            return cls.getMethod(fld.getName(), fld.getType()).getReturnType() == void.class;
        }
        catch (NoSuchMethodException e) {
            return false;
        }
    }

    /** */
    private static boolean writeExternalDataMethodExists(Class<?> cls) {
        while (cls != IgniteDataTransferObject.class) {
            try {
                Method mtd = cls.getDeclaredMethod(WRITE_MTD, ObjectOutput.class);

                return !Modifier.isAbstract(mtd.getModifiers());
            }
            catch (NoSuchMethodException e) {
                cls = cls.getSuperclass();
            }
        }

        return false;
    }
}
