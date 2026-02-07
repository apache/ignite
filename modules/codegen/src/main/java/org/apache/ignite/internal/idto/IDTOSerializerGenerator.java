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

package org.apache.ignite.internal.idto;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiFunction;
import javax.annotation.processing.FilerException;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.NestingKind;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.MessageSerializerGenerator.NL;
import static org.apache.ignite.internal.MessageSerializerGenerator.TAB;
import static org.apache.ignite.internal.MessageSerializerGenerator.enumType;
import static org.apache.ignite.internal.MessageSerializerGenerator.identicalFileIsAlreadyGenerated;
import static org.apache.ignite.internal.idto.IgniteDataTransferObjectProcessor.DTO_CLASS;

/**
 * Generates serializer class for given {@code IgniteDataTransferObject} extension.
 * The generated serializer follows the naming convention:
 * {@code org.apache.ignite.internal.codegen.[IDTOClassName]Serializer}.
 */
public class IDTOSerializerGenerator {
    /** Serializer interface. */
    public static final String DTO_SERDES_INTERFACE = "org.apache.ignite.internal.dto.IgniteDataTransferObjectSerializer";

    /** Class javadoc. */
    static final String CLS_JAVADOC = "/** " + NL +
        " * This class is generated automatically." + NL +
        " *" + NL +
        " * @see org.apache.ignite.internal.dto.IgniteDataTransferObject" + NL +
        " */";

    /** */
    private static final IgniteBiTuple<String, String> OBJECT_SERDES =
        F.t("out.writeObject(obj.${f});", "(${c})in.readObject()");

    /** */
    private static final IgniteBiTuple<String, String> STR_STR_MAP =
        F.t("U.writeStringMap(out, obj.${f});", "U.readStringMap(in)");

    /** Type name to write/read code for the type. */
    private static final Map<String, IgniteBiTuple<String, String>> TYPE_SERDES = new HashMap<>();

    {
        TYPE_SERDES.put(boolean.class.getName(), F.t("out.writeBoolean(obj.${f});", "in.readBoolean()"));
        TYPE_SERDES.put(byte.class.getName(), F.t("out.write(obj.${f});", "in.read()"));
        TYPE_SERDES.put(short.class.getName(), F.t("out.writeShort(obj.${f});", "in.readShort()"));
        TYPE_SERDES.put(int.class.getName(), F.t("out.writeInt(obj.${f});", "in.readInt()"));
        TYPE_SERDES.put(long.class.getName(), F.t("out.writeLong(obj.${f});", "in.readLong()"));
        TYPE_SERDES.put(float.class.getName(), F.t("out.writeFloat(obj.${f});", "in.readFloat()"));
        TYPE_SERDES.put(double.class.getName(), F.t("out.writeDouble(obj.${f});", "in.readDouble()"));

        TYPE_SERDES.put(Boolean.class.getName(), OBJECT_SERDES);
        TYPE_SERDES.put(Byte.class.getName(), OBJECT_SERDES);
        TYPE_SERDES.put(Short.class.getName(), OBJECT_SERDES);
        TYPE_SERDES.put(Integer.class.getName(), OBJECT_SERDES);
        TYPE_SERDES.put(Long.class.getName(), OBJECT_SERDES);
        TYPE_SERDES.put(Float.class.getName(), OBJECT_SERDES);
        TYPE_SERDES.put(Double.class.getName(), OBJECT_SERDES);
        TYPE_SERDES.put(Throwable.class.getName(), OBJECT_SERDES);
        TYPE_SERDES.put(Exception.class.getName(), OBJECT_SERDES);
        TYPE_SERDES.put(Object.class.getName(), OBJECT_SERDES);

        TYPE_SERDES.put(String.class.getName(), F.t("U.writeString(out, obj.${f});", "U.readString(in)"));
        TYPE_SERDES.put(UUID.class.getName(), F.t("U.writeUuid(out, obj.${f});", "U.readUuid(in)"));
        TYPE_SERDES.put("org.apache.ignite.lang.IgniteUuid", F.t("U.writeIgniteUuid(out, obj.${f});", "U.readIgniteUuid(in)"));
        TYPE_SERDES.put("org.apache.ignite.internal.processors.cache.version.GridCacheVersion", OBJECT_SERDES);
        TYPE_SERDES.put("org.apache.ignite.lang.IgniteProductVersion", OBJECT_SERDES);
        TYPE_SERDES.put("org.apache.ignite.internal.binary.BinaryMetadata", OBJECT_SERDES);
        TYPE_SERDES.put("org.apache.ignite.internal.management.cache.PartitionKey", OBJECT_SERDES);
        TYPE_SERDES.put("org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion", OBJECT_SERDES);
        TYPE_SERDES.put("org.apache.ignite.cluster.ClusterNode", OBJECT_SERDES);
        TYPE_SERDES.put("org.apache.ignite.cache.CacheMode",
            F.t("out.writeByte(CacheMode.toCode(obj.${f}));", "CacheMode.fromCode(in.readByte())"));

        TYPE_SERDES.put(TreeMap.class.getName(), F.t("U.writeMap(out, obj.${f});", "U.readTreeMap(in)"));
        TYPE_SERDES.put(LinkedHashMap.class.getName(), F.t("U.writeMap(out, obj.${f});", "U.readLinkedMap(in)"));
        TYPE_SERDES.put(Map.class.getName(), F.t("U.writeMap(out, obj.${f});", "U.readMap(in)"));
        TYPE_SERDES.put(Collection.class.getName(), F.t("U.writeCollection(out, obj.${f});", "U.readCollection(in)"));
        TYPE_SERDES.put(List.class.getName(), F.t("U.writeCollection(out, obj.${f});", "U.readList(in)"));
        TYPE_SERDES.put(Set.class.getName(), F.t("U.writeCollection(out, obj.${f});", "U.readSet(in)"));
    }

    /** Write/Read code for enum. */
    private static final IgniteBiTuple<String, String> ENUM_SERDES =
        F.t("U.writeEnum(out, obj.${f});", "U.readEnum(in, ${c}.class)");

    /** Write/Read code for array. */
    private static final IgniteBiTuple<String, String> OBJ_ARRAY_SERDES =
        F.t("U.writeArray(out, obj.${f});", "U.readArray(in, ${c}.class)");

    /** Type name to write/read code for the array of type. */
    private static final Map<String, IgniteBiTuple<String, String>> ARRAY_TYPE_SERDES = new HashMap<>();

    {
        ARRAY_TYPE_SERDES.put(byte.class.getName(), F.t("U.writeByteArray(out, obj.${f});", "U.readByteArray(in)"));
        ARRAY_TYPE_SERDES.put(int.class.getName(), F.t("U.writeIntArray(out, obj.${f});", "U.readIntArray(in)"));
        ARRAY_TYPE_SERDES.put(long.class.getName(), F.t("U.writeLongArray(out, obj.${f});", "U.readLongArray(in)"));
    }

    /** Environment. */
    private final ProcessingEnvironment env;

    /** Type to generated serializer for. */
    private final TypeElement type;

    /** Serializer imports. */
    private final Set<String> imports = new HashSet<>();

    /**
     * @param env Environment.
     * @param type Type to generate serializer for.
     */
    public IDTOSerializerGenerator(ProcessingEnvironment env, TypeElement type) {
        this.env = env;
        this.type = type;
    }

    /** @return Fully qualified name for generated class. */
    public String serializerFQN() {
        TypeElement topLevelCls = type;

        while (topLevelCls.getNestingKind() != NestingKind.TOP_LEVEL)
            topLevelCls = (TypeElement)topLevelCls.getEnclosingElement();

        PackageElement pkg = (PackageElement)topLevelCls.getEnclosingElement();

        return pkg.getQualifiedName().toString() + "." + serializerName();
    }

    /**
     * @return {@code True} if generation succeed.
     * @throws Exception in case of error.
     */
    public boolean generate() throws Exception {
        String fqnClsName = serializerFQN();
        String serCode = generateSerializerCode();

        try {
            JavaFileObject file = env.getFiler().createSourceFile(fqnClsName);

            try (Writer writer = file.openWriter()) {
                writer.append(serCode);
                writer.flush();
            }

            return true;
        }
        catch (FilerException e) {
            // IntelliJ IDEA parses Ignite's pom.xml and configures itself to use this annotation processor on each Run.
            // During a Run, it invokes the processor and may fail when attempting to generate sources that already exist.
            // There is no a setting to disable this invocation. The IntelliJ community suggests a workaround — delegating
            // all Run commands to Maven. However, this significantly slows down test startup time.
            // This hack checks whether the content of a generating file is identical to already existed file, and skips
            // handling this class if it is.
            if (!identicalFileIsAlreadyGenerated(env, serCode, fqnClsName)) {
                env.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    fqnClsName + " is already generated. Try 'mvn clean install' to fix the issue.");

                throw e;
            }

            return false;
        }
    }

    /** @return Code for the calss implementing {@code org.apache.ignite.internal.dto.IgniteDataTransferObjectSerializer}. */
    private String generateSerializerCode() throws IOException {
        imports.add(DTO_SERDES_INTERFACE);
        imports.add(ObjectOutput.class.getName());
        imports.add(ObjectInput.class.getName());
        imports.add(IOException.class.getName());
        imports.add("org.apache.ignite.internal.util.typedef.internal.U");

        if (type.getNestingKind() != NestingKind.TOP_LEVEL)
            imports.add(type.getQualifiedName().toString());

        String simpleClsName = String.valueOf(type.getSimpleName());

        Collection<VariableElement> flds = fields(type);

        List<String> write = generateWrite(simpleClsName, flds);
        List<String> read = generateRead(simpleClsName, flds);

        try (Writer writer = new StringWriter()) {
            writeClassHeader(writer, simpleClsName);

            for (String line : write) {
                writer.write(TAB);
                writer.write(line);
                writer.write(NL);
            }

            writer.write(NL);
            for (String line : read) {
                writer.write(TAB);
                writer.write(line);
                writer.write(NL);
            }

            writer.write("}");
            writer.write(NL);

            return writer.toString();
        }
    }

    /**
     * @param writer Writer to write class to.
     * @param simpleClsName Class name
     * @throws IOException  In case of error.
     */
    private void writeClassHeader(Writer writer, String simpleClsName) throws IOException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("license.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            PrintWriter out = new PrintWriter(writer);

            String line;

            while ((line = reader.readLine()) != null)
                out.println(line);
        }

        writer.write(NL);
        writer.write("package " + env.getElementUtils().getPackageOf(type).toString() + ";" + NL + NL);

        for (String regularImport: imports)
            writer.write("import " + regularImport + ";" + NL);

        writer.write(NL);
        writer.write(CLS_JAVADOC);
        writer.write(NL);
        writer.write("public class " + serializerName() + " implements " + simpleName(DTO_SERDES_INTERFACE) +
            "<" + simpleClsName + "> {" + NL);
    }

    /** @return Lines for generated {@code IgniteDataTransferObjectSerializer#writeExternal(T, ObjectOutput)} method. */
    private List<String> generateWrite(String clsName, Collection<VariableElement> flds) {
        List<String> code = new ArrayList<>();

        code.add("/** {@inheritDoc} */");
        code.add("@Override public void writeExternal(" + clsName + " obj, ObjectOutput out) throws IOException {");

        fieldsSerdes(flds, (t, noNull) -> t.get1()).forEach(line -> code.add(TAB + line));

        code.add("}");

        return code;
    }

    /** @return Lines for generated {@code IgniteDataTransferObjectSerializer#readExternal(T, ObjectInput)} method. */
    private List<String> generateRead(String clsName, Collection<VariableElement> flds) {
        List<String> code = new ArrayList<>();

        code.add("/** {@inheritDoc} */");
        code.add("@Override public void readExternal(" + clsName + " obj, ObjectInput in) throws IOException, ClassNotFoundException {");
        fieldsSerdes(flds, (t, notNull) -> {
            String pattern = "obj.${f} = " + t.get2() + ";";

            if (notNull) {
                /**
                 * Intention here to change `obj.field = U.readSomething();` line to:
                 * ```
                 *    Type field0 = U.readSomething();
                 *    if (field0 != null)
                 *        obj.field = field0;
                 * ```
                 * We want to respect @NotNull annotation and keep default value.
                 */
                pattern = pattern.replaceAll("obj.\\$\\{f}", "\\${c} \\${f}0");
                pattern += "\nif (${f}0 != null)\n" + TAB + "obj.${f} = ${f}0;";
            }

            return pattern;
        }).forEach(line -> {
            if (line.indexOf('\n') != -1) {
                for (String line0 : line.split("\n"))
                    code.add(TAB + line0);
            }
            else
                code.add(TAB + line);
        });
        code.add("}");

        return code;
    }

    /**
     * @param flds Fields to generated serdes for.
     * @param lineProvider Function to generated serdes code for the field.
     * @return Lines to serdes fields.
     */
    private List<String> fieldsSerdes(
        Collection<VariableElement> flds,
        BiFunction<IgniteBiTuple<String, String>, Boolean, String> lineProvider
    ) {
        TypeMirror dtoCls = env.getElementUtils().getTypeElement(DTO_CLASS).asType();

        List<String> code = new ArrayList<>();

        for (VariableElement fld : flds) {
            TypeMirror type = fld.asType();
            TypeMirror comp = null;

            boolean notNull = type.toString().startsWith("@" + NotNull.class.getName());

            IgniteBiTuple<String, String> serDes = null;

            if (env.getTypeUtils().isAssignable(type, dtoCls))
                serDes = OBJECT_SERDES;
            else if (type.getKind() == TypeKind.TYPEVAR)
                serDes = F.t("out.writeObject(obj.${f});", "in.readObject()");
            else if (type.getKind() == TypeKind.ARRAY) {
                comp = ((ArrayType)type).getComponentType();

                serDes = ARRAY_TYPE_SERDES.get(className(comp));

                if (serDes == null) {
                    System.out.println("comp.toString() = " + comp.toString());
                    if (enumType(env, comp))
                        serDes = OBJ_ARRAY_SERDES;
                    else if (TYPE_SERDES.containsKey(comp.toString())) {
                        IgniteBiTuple<String, String> arrElSerdes = TYPE_SERDES.get(comp.toString());

                        String writeStr = arrElSerdes.get1().replaceAll("obj.\\$\\{f}", "el");

                        String write = "{\n" +
                            TAB + "int len = obj.${f} == null ? 0 : obj.${f}.length;\n" +
                            TAB + "out.writeInt(len);\n" +
                            TAB + "if (obj.${f} != null && len > 0) {\n" +
                            TAB + TAB + "for (int i = 0; i < len; i++) {\n" +
                            TAB + TAB + TAB + "${c} el = obj.${f}[i];\n" +
                            TAB + TAB + TAB + writeStr + "\n" +
                            TAB + TAB + "}\n" +
                            TAB + "}\n" +
                            "}";

                        serDes = F.t(write, "null; // FIXME");
                    }
                    else
                        serDes = OBJ_ARRAY_SERDES;
                }
            }
            else {
                if (className(type).equals(Map.class.getName())) {
                    TypeMirror strCls = env.getElementUtils().getTypeElement(String.class.getName()).asType();

                    DeclaredType dt = (DeclaredType)type;

                    List<? extends TypeMirror> ta = dt.getTypeArguments();

                    if (ta.size() == 2
                        && env.getTypeUtils().isAssignable(ta.get(0), strCls)
                        && env.getTypeUtils().isAssignable(ta.get(1), strCls)) {
                        serDes = STR_STR_MAP;
                    }
                }

                if (serDes == null) {
                    serDes = TYPE_SERDES.get(className(type));

                    if (serDes == null && enumType(env, type))
                        serDes = ENUM_SERDES;
                }
            }

            if (serDes == null)
                throw new IllegalStateException("Unsupported type: " + type);

            String pattern = lineProvider.apply(serDes, notNull);

            code.add(pattern
                .replaceAll("\\$\\{f}", fld.getSimpleName().toString())
                .replaceAll("\\$\\{c}", simpleClassName(comp == null ? type : comp)));
        }

        return code;
    }

    /** @return List of non-static and non-transient field for given {@code type}. */
    private Collection<VariableElement> fields(TypeElement type) {
        SortedMap<Integer, VariableElement> flds = new TreeMap<>();

        while (type != null) {
            for (Element el: type.getEnclosedElements()) {
                if (el.getKind() != ElementKind.FIELD)
                    continue;

                if (el.getModifiers().contains(Modifier.STATIC) || el.getModifiers().contains(Modifier.TRANSIENT))
                    continue;

                Order order = el.getAnnotation(Order.class);

                if (order == null) {
                    throw new IllegalStateException("Please, add @Order " +
                        "[field=" + el.getSimpleName() + ", cls=" + type.getQualifiedName() + "]");
                }

                VariableElement prev = flds.put(order.value(), (VariableElement)el);

                if (prev != null) {
                    throw new IllegalStateException("Duplicate @Order for " + type.getQualifiedName() + ": " +
                        "[order=" + order.value() + ", fld1=" + prev.getSimpleName() + ", fld2 = " + el.getSimpleName() + ']');
                }
            }

            Element superType = env.getTypeUtils().asElement(type.getSuperclass());

            type = (TypeElement)superType;
        }

        for (int i = 0; i < flds.size(); i++) {
            if (!flds.containsKey(i))
                throw new IllegalStateException("@Order not found: " + i);
        }

        return flds.values();
    }

    /** @return FQN of {@code comp}. */
    private static String className(TypeMirror comp) {
        String n = comp.toString();

        int spaceIdx = n.indexOf(' ');

        if (spaceIdx != -1)
            n = n.substring(spaceIdx + 1);

        int genIdx = n.indexOf('<');

        return genIdx == -1 ? n : n.substring(0, genIdx);
    }

    /** @return Serializer class name. */
    private String serializerName() {
        return type.getSimpleName() + "Serializer";
    }

    /**
     * Adds to imports if class need to be imported explicitly.
     *
     * @return Simple class name.
     */
    private String simpleClassName(TypeMirror type) {
        if (type instanceof PrimitiveType)
            return className(type);

        String fqn = className(type);

        if (!fqn.startsWith("java.lang") && type.getKind() != TypeKind.TYPEVAR)
            imports.add(fqn);

        return simpleName(fqn);
    }

    /** @return Simple class name. */
    public static String simpleName(String fqn) {
        return fqn.substring(fqn.lastIndexOf('.') + 1);
    }
}
