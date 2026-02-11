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
import java.io.Serializable;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
        F.t("out.writeObject(${var})", "(${Type})in.readObject()");

    /** */
    private static final IgniteBiTuple<String, String> STR_STR_MAP =
        F.t("U.writeStringMap(out, ${var})", "U.readStringMap(in)");

    /** Type name to write/read code for the type. */
    private static final Map<String, IgniteBiTuple<String, String>> TYPE_SERDES = new HashMap<>();

    {
        TYPE_SERDES.put(boolean.class.getName(), F.t("out.writeBoolean(${var})", "in.readBoolean()"));
        TYPE_SERDES.put(byte.class.getName(), F.t("out.write(${var})", "in.read()"));
        TYPE_SERDES.put(short.class.getName(), F.t("out.writeShort(${var})", "in.readShort()"));
        TYPE_SERDES.put(int.class.getName(), F.t("out.writeInt(${var})", "in.readInt()"));
        TYPE_SERDES.put(long.class.getName(), F.t("out.writeLong(${var})", "in.readLong()"));
        TYPE_SERDES.put(float.class.getName(), F.t("out.writeFloat(${var})", "in.readFloat()"));
        TYPE_SERDES.put(double.class.getName(), F.t("out.writeDouble(${var})", "in.readDouble()"));

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

        TYPE_SERDES.put(String.class.getName(), F.t("U.writeString(out, ${var})", "U.readString(in)"));
        TYPE_SERDES.put(UUID.class.getName(), F.t("U.writeUuid(out, ${var})", "U.readUuid(in)"));
        TYPE_SERDES.put("org.apache.ignite.lang.IgniteUuid", F.t("U.writeIgniteUuid(out, ${var});", "U.readIgniteUuid(in)"));
        TYPE_SERDES.put("org.apache.ignite.internal.processors.cache.version.GridCacheVersion", OBJECT_SERDES);
        TYPE_SERDES.put("org.apache.ignite.lang.IgniteProductVersion", OBJECT_SERDES);
        TYPE_SERDES.put("org.apache.ignite.internal.binary.BinaryMetadata", OBJECT_SERDES);
        TYPE_SERDES.put("org.apache.ignite.internal.management.cache.PartitionKey", OBJECT_SERDES);
        TYPE_SERDES.put("org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion", OBJECT_SERDES);
        TYPE_SERDES.put("org.apache.ignite.cluster.ClusterNode", OBJECT_SERDES);
        TYPE_SERDES.put("org.apache.ignite.cache.CacheMode",
            F.t("out.writeByte(CacheMode.toCode(${var}));", "CacheMode.fromCode(in.readByte())"));
    }

    /** Write/Read code for enum. */
    private static final IgniteBiTuple<String, String> ENUM_SERDES =
        F.t("U.writeEnum(out, ${var})", "U.readEnum(in, ${Type}.class)");

    /** Write/Read code for array. */
    private static final IgniteBiTuple<String, String> OBJ_ARRAY_SERDES =
        F.t("U.writeArray(out, ${var})", "U.readArray(in, ${Type}.class)");

    /** Type name to write/read code for the array of type. */
    private static final Map<String, IgniteBiTuple<String, String>> ARRAY_TYPE_SERDES = new HashMap<>();

    {
        ARRAY_TYPE_SERDES.put(byte.class.getName(), F.t("U.writeByteArray(out, ${var})", "U.readByteArray(in)"));
        ARRAY_TYPE_SERDES.put(char.class.getName(), F.t("U.writeCharArray(out, ${var})", "U.readCharArray(in)"));
        ARRAY_TYPE_SERDES.put(int.class.getName(), F.t("U.writeIntArray(out, ${var})", "U.readIntArray(in)"));
        ARRAY_TYPE_SERDES.put(long.class.getName(), F.t("U.writeLongArray(out, ${var})", "U.readLongArray(in)"));
    }

    /** Interface to implementations. */
    private static final Map<String, String> COLL_IMPL = new HashMap<>();

    {
        COLL_IMPL.put(Collection.class.getName(), ArrayList.class.getName());
        COLL_IMPL.put(List.class.getName(), ArrayList.class.getName());
        COLL_IMPL.put(Set.class.getName(), HashSet.class.getName());
    }

    /** Environment. */
    private final ProcessingEnvironment env;

    /** Serializable type. */
    private final TypeMirror serializableCls;

    /** Dto type. */
    private final TypeMirror dtoCls;

    /** Want to generate specialized code for collections. */
    private final TypeMirror coll;

    /** Want to generate specialized code for maps. */
    private final TypeMirror map;

    /** IgniteBiTuple extends Map (sic!) and placed in public API (sic!) so want to generate specialized code for it. */
    private final TypeMirror biTuple;

    /** Type to generated serializer for. */
    private final TypeElement type;

    /** Serializer imports. */
    private final Set<String> imports = new HashSet<>();

    /** If {@code True} then write method generated now. */
    private boolean write;

    /** Nesting level. */
    private int level;

    /**
     * @param env Environment.
     * @param type Type to generate serializer for.
     */
    public IDTOSerializerGenerator(ProcessingEnvironment env, TypeElement type) {
        this.env = env;
        this.type = type;

        serializableCls = env.getElementUtils().getTypeElement(Serializable.class.getName()).asType();
        dtoCls = env.getElementUtils().getTypeElement(DTO_CLASS).asType();
        coll = env.getElementUtils().getTypeElement(Collection.class.getName()).asType();
        map = env.getElementUtils().getTypeElement(Map.class.getName()).asType();
        biTuple = env.getTypeUtils().erasure(env.getElementUtils().getTypeElement(IgniteBiTuple.class.getName()).asType());
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

        Collection<VariableElement> flds = fields(type);

        List<String> write = generateWrite(flds);
        List<String> read = generateRead(flds);

        try (Writer writer = new StringWriter()) {
            writeClassHeader(writer);

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
     * @throws IOException  In case of error.
     */
    private void writeClassHeader(Writer writer) throws IOException {
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
            "<" + typeWithGeneric(type.asType()) + "> {" + NL);
    }

    /** @return Lines for generated {@code IgniteDataTransferObjectSerializer#writeExternal(T, ObjectOutput)} method. */
    private List<String> generateWrite(Collection<VariableElement> flds) {
        write = true;

        List<String> code = new ArrayList<>();

        code.add("/** {@inheritDoc} */");
        code.add("@Override public void writeExternal(" + typeWithGeneric(type.asType()) + " obj, ObjectOutput out) throws IOException {");

        fieldsSerdes(flds).forEach(line -> code.add(TAB + line));

        code.add("}");

        return code;
    }

    /** @return Lines for generated {@code IgniteDataTransferObjectSerializer#readExternal(T, ObjectInput)} method. */
    private List<String> generateRead(Collection<VariableElement> flds) {
        write = false;

        List<String> code = new ArrayList<>();

        code.add("/** {@inheritDoc} */");
        code.add("@Override public void readExternal(" + typeWithGeneric(type.asType()) + " obj, ObjectInput in) " +
            "throws IOException, ClassNotFoundException {");

        fieldsSerdes(flds).forEach(line -> code.add(TAB + line));

        code.add("}");

        return code;
    }

    /**
     * @param flds Fields to generated serdes for.
     * @return Lines to serdes fields.
     */
    private List<String> fieldsSerdes(Collection<VariableElement> flds) {
        return flds.stream()
            .flatMap(fld -> variableCode(fld.asType(), "obj." + fld.getSimpleName().toString()))
            .collect(Collectors.toList());
    }

    /**
     * @param type Type of variable.
     * @param var Name of the variable.
     */
    private Stream<String> variableCode(
        TypeMirror type,
        String var
    ) {
        IgniteBiTuple<String, String> serDes;

        if (isArray(type))
            return arrayCode(type, var);
        else if (isCollection(type))
            return collectionCode(type, var);
        else if (isBiTuple(type))
            return biTupleCode(type, var);
        else if (isMap(type))
            return mapCode(type, var);
        else if (isDto(type))
            serDes = OBJECT_SERDES;
        else if (type.getKind() == TypeKind.TYPEVAR)
            serDes = F.t("out.writeObject(${var})", "in.readObject()");
        else {
            serDes = TYPE_SERDES.get(className(type));

            if (serDes == null) {
                if (enumType(env, type))
                    serDes = ENUM_SERDES;
                else if (isSerializable(type))
                    serDes = OBJECT_SERDES;
            }
        }

        throwIfNull(type, serDes);

        boolean nullable = !type.toString().startsWith("@" + NotNull.class.getName());

        if (nullable || write)
            return simpleSerdes(serDes, var, type);

        /**
         * Intention here to change `obj.field = U.readSomething();` line to:
         * ```
         * {
         *    Type maybeNull = U.readSomething();
         *    if (maybeNull != null)
         *        obj.field = maybeNull;
         * }
         * ```
         * We want to respect @NotNull annotation and keep default value.
         */
        return Stream.of("{",
            TAB + "${Type} maybeNull = " + serDes.get2() + ";",
            TAB + "if (maybeNull != null)",
            TAB + TAB + "${var} = maybeNull;",
            "}"
        ).map(line -> replacePlaceholders(line, var, type));
    }

    /**
     * @param type IgniteBiTuple type.
     * @param var Variable to read(write) from(to).
     * @return Serdes code for IgniteBiTuple.
     */
    private Stream<String> biTupleCode(TypeMirror type, String var) {
        DeclaredType dt = (DeclaredType)type;

        List<? extends TypeMirror> ta = dt.getTypeArguments();

        assert ta.size() == 2;

        TypeMirror keyType = ta.get(0);
        TypeMirror valType = ta.get(1);

        Stream<String> res;

        String k = "k" + (level == 0 ? "" : level);
        String v = "v" + (level == 0 ? "" : level);

        Map<String, String> params = Map.of(
            "${var}", var,
            "${KeyType}", typeWithGeneric(keyType),
            "${ValType}", typeWithGeneric(valType),
            "${len}", "len" + (level == 0 ? "" : level),
            "${k}", k,
            "${v}", v
        );

        level++;

        if (write) {
            res = Stream.of("{",
                TAB + "${KeyType} ${k} = ${var}.get1();",
                TAB + "${ValType} ${v} = ${var}.get2();");

            res = Stream.concat(res, variableCode(keyType, k).map(line -> TAB + line));
            res = Stream.concat(res, variableCode(valType, v).map(line -> TAB + line));
            res = Stream.concat(res, Stream.of("}"));
        }
        else {
            imports.add(IgniteBiTuple.class.getName());

            res = Stream.of("{",
                TAB + "${KeyType} ${k} = null;",
                TAB + "${ValType} ${v} = null;");

            res = Stream.concat(res, variableCode(keyType, k).map(line -> TAB + line));
            res = Stream.concat(res, variableCode(valType, v).map(line -> TAB + line));
            res = Stream.concat(res, Stream.of(TAB + "${var} = new IgniteBiTuple<>(${k}, ${v});", "}"));
        }

        level--;

        return res.map(line -> replacePlaceholders(line, params));
    }

    /**
     * @param type Map type.
     * @param var Variable to read(write) from(to).
     * @return Serdes code for map.
     */
    private Stream<String> mapCode(TypeMirror type, String var) {
        IgniteBiTuple<String, String> serDes = null;

        DeclaredType dt = (DeclaredType)type;

        List<? extends TypeMirror> ta = dt.getTypeArguments();

        assert ta.size() == 2;

        TypeMirror keyType = ta.get(0);
        TypeMirror valType = ta.get(1);
        TypeMirror strCls = env.getElementUtils().getTypeElement(String.class.getName()).asType();

        if (className(type).equals(Map.class.getName()) && assignableFrom(keyType, strCls) && assignableFrom(valType, strCls))
            serDes = STR_STR_MAP;
        else if (className(type).equals(TreeMap.class.getName()))
            serDes = F.t("U.writeMap(out, ${var})", "U.readTreeMap(in)");
        else if (className(type).equals(LinkedHashMap.class.getName()))
            serDes = F.t("U.writeMap(out, ${var})", "U.readLinkedMap(in)");

        if (serDes == null && !hasCustomSerdes(keyType) && !hasCustomSerdes(valType))
            serDes = F.t("U.writeMap(out, ${var})", "U.readMap(in)");

        // Map special case or Ignite can't serialize map entries efficiently.
        if (serDes != null)
            return simpleSerdes(serDes, var, type);

        Stream<String> res;

        String el = "el" + (level == 0 ? "" : level);
        String k = "k" + (level == 0 ? "" : level);
        String v = "v" + (level == 0 ? "" : level);

        Map<String, String> params = Map.of(
            "${var}", var,
            "${KeyType}", typeWithGeneric(keyType),
            "${ValType}", typeWithGeneric(valType),
            "${len}", "len" + (level == 0 ? "" : level),
            "${i}", "i" + (level == 0 ? "" : level),
            "${k}", k,
            "${v}", v,
            "${el}", el
        );

        level++;

        if (write) {
            imports.add(Map.class.getName());
            imports.add(Map.Entry.class.getName().replace('$', '.'));

            res = Stream.of("{",
                TAB + "int ${len} = ${var} == null ? -1 : ${var}.size();",
                TAB + "out.writeInt(${len});",
                TAB + "if (${len} > 0) {",
                TAB + TAB + "for (Map.Entry<${KeyType}, ${ValType}> ${el} : ${var}.entrySet()) {",
                TAB + TAB + TAB + "${KeyType} ${k} = ${el}.getKey();",
                TAB + TAB + TAB + "${ValType} ${v} = ${el}.getValue();");

            res = Stream.concat(res, variableCode(keyType, k).map(line -> TAB + TAB + TAB + line));
            res = Stream.concat(res, variableCode(valType, v).map(line -> TAB + TAB + TAB + line));
            res = Stream.concat(res, Stream.of(TAB + TAB + "}", TAB + "}", "}"));
        }
        else {
            imports.add(HashMap.class.getName());

            res = Stream.of("{",
                TAB + "int ${len} = in.readInt();",
                TAB + "if (${len} >= 0) {",
                TAB + TAB + "${var} = new HashMap<>();",
                TAB + TAB + "for (int ${i} = 0; ${i} < ${len}; ${i}++) {",
                TAB + TAB + TAB + "${KeyType} ${k} = null;",
                TAB + TAB + TAB + "${ValType} ${v} = null;");

            res = Stream.concat(res, variableCode(keyType, k).map(line -> TAB + TAB + TAB + line));
            res = Stream.concat(res, variableCode(valType, v).map(line -> TAB + TAB + TAB + line));
            res = Stream.concat(res, Stream.of(TAB + TAB + TAB + "${var}.put(${k}, ${v});"));
            res = Stream.concat(res, Stream.of(TAB + TAB + "}", TAB + "}", "}"));
        }

        level--;

        return res.map(line -> replacePlaceholders(line, params));
    }

    /**
     * @param type Collection type.
     * @param var Variable to read(write) from(to).
     * @return Serdes code for collection.
     */
    private Stream<String> collectionCode(TypeMirror type, String var) {
        DeclaredType dt = (DeclaredType)type;

        assert dt.getTypeArguments().size() == 1;

        TypeMirror colEl = dt.getTypeArguments().get(0);

        if (!hasCustomSerdes(colEl)) {
            // Ignite can't serialize collections elements efficiently.
            IgniteBiTuple<String, String> serDes = null;

            if (Collection.class.getName().equals(className(type)))
                serDes = F.t("U.writeCollection(out, ${var})", "U.readCollection(in)");
            else if (List.class.getName().equals(className(type)))
                serDes = F.t("U.writeCollection(out, ${var})", "U.readList(in)");
            else if (Set.class.getName().equals(className(type)))
                serDes = F.t("U.writeCollection(out, ${var})", "U.readSet(in)");

            throwIfNull(type, serDes);

            return simpleSerdes(serDes, var, type);
        }

        Stream<String> res;

        String el = "el" + (level == 0 ? "" : level);

        Map<String, String> params = Map.of(
            "${var}", var,
            "${Type}", colEl.toString(),
            "${CollectionImpl}", simpleName(COLL_IMPL.get(className(type))),
            "${len}", "len" + (level == 0 ? "" : level),
            "${i}", "i" + (level == 0 ? "" : level),
            "${el}", el
        );

        level++;

        if (write) {
            res = Stream.of("{",
                TAB + "int ${len} = ${var} == null ? -1 : ${var}.size();",
                TAB + "out.writeInt(${len});",
                TAB + "if (${len} > 0) {",
                TAB + TAB + "for (${Type} ${el} : ${var}) {");

            res = Stream.concat(res, variableCode(colEl, el).map(line -> TAB + TAB + TAB + line));
            res = Stream.concat(res, Stream.of(TAB + TAB + "}", TAB + "}", "}"));
        }
        else {
            String implCls = COLL_IMPL.get(className(type));

            imports.add(implCls);

            assert implCls != null;

            res = Stream.of("{",
                TAB + "int ${len} = in.readInt();",
                TAB + "if (${len} >= 0) {",
                TAB + TAB + "${var} = new ${CollectionImpl}<>();",
                TAB + TAB + "for (int ${i} = 0; ${i} < ${len}; ${i}++) {",
                TAB + TAB + TAB + "${Type} ${el} = null;");

            res = Stream.concat(res, variableCode(colEl, el).map(line -> TAB + TAB + TAB + line));
            res = Stream.concat(res, Stream.of(TAB + TAB + TAB + "${var}.add(${el});"));
            res = Stream.concat(res, Stream.of(TAB + TAB + "}", TAB + "}", "}"));
        }

        level--;

        return res.map(line -> replacePlaceholders(line, params));
    }

    /**
     * @param type Array type.
     * @param var Variable to read(write) from(to).
     * @return Serdes code for array.
     */
    private Stream<String> arrayCode(TypeMirror type, String var) {
        TypeMirror comp = ((ArrayType)type).getComponentType();

        IgniteBiTuple<String, String> arrSerdes = ARRAY_TYPE_SERDES.get(className(comp));

        if (arrSerdes == null && !hasCustomSerdes(comp)) {
            // Ignite can't serialize array element efficiently.
            arrSerdes = OBJ_ARRAY_SERDES;
        }

        if (arrSerdes != null)
            return simpleSerdes(arrSerdes, var, comp);

        Stream<String> res;

        if (write) {
            res = Stream.of("{",
                TAB + "int ${len} = ${var} == null ? -1 : ${var}.length;",
                TAB + "out.writeInt(${len});",
                TAB + "if (${len} > 0) {",
                TAB + TAB + "for (int ${i} = 0; ${i} < ${len}; ${i}++) {");

            res = Stream.concat(res, variableCode(comp, var + "[${i}]").map(line -> TAB + TAB + TAB + line));
            res = Stream.concat(res, Stream.of(TAB + TAB + "}", TAB + "}", "}"));
        }
        else {
            res = Stream.of("{",
                TAB + "int ${len} = in.readInt();",
                TAB + "if (${len} >= 0) {",
                TAB + TAB + "${var} = new ${Type}[${len}];",
                TAB + TAB + "for (int ${i} = 0; ${i} < ${len}; ${i}++) {");

            res = Stream.concat(res, variableCode(comp, var + "[${i}]").map(line -> TAB + TAB + TAB + line));
            res = Stream.concat(res, Stream.of(TAB + TAB + "}", TAB + "}", "}"));
        }

        return res.map(line -> replacePlaceholders(line, Map.of(
            "${var}", var,
            "${Type}", simpleClassName(comp),
            "${len}", "len" + (level == 0 ? "" : level),
            "${i}", "i" + (level == 0 ? "" : level)
        )));
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

        if (!fqn.startsWith("java.lang") && type.getKind() != TypeKind.TYPEVAR && type.getKind() != TypeKind.ARRAY)
            imports.add(fqn);

        return simpleName(fqn);
    }

    /** @return Simple class name. */
    private String typeWithGeneric(TypeMirror tp) {
        if (tp.getKind() == TypeKind.TYPEVAR || tp.getKind() == TypeKind.WILDCARD)
            return "Object";

        if (!(tp instanceof DeclaredType))
            return simpleClassName(tp);

        DeclaredType dt = (DeclaredType)tp;

        if (F.size(dt.getTypeArguments()) == 0)
            return simpleClassName(tp);

        StringBuilder generic = new StringBuilder("<");

        for (int i = 0; i < F.size(dt.getTypeArguments()); i++) {
            TypeMirror gt = dt.getTypeArguments().get(i);

            if (i > 0)
                generic.append(", ");

            generic.append((gt.getKind() == TypeKind.TYPEVAR || gt.getKind() == TypeKind.WILDCARD)
                ? "Object"
                : gt.toString());
        }

        return simpleClassName(tp) + generic.append(">");
    }

    /** @return Simple class name. */
    public static String simpleName(String fqn) {
        return fqn.substring(fqn.lastIndexOf('.') + 1);
    }

    /** Replaces placeholders to current values. */
    private String replacePlaceholders(String line, String var, TypeMirror type) {
        return replacePlaceholders(line, Map.of("${var}", var, "${Type}", simpleClassName(type)));
    }

    /** Replaces placeholders to current values. */
    private String replacePlaceholders(String line, Map<String, String> subst) {
        for (Map.Entry<String, String> e : subst.entrySet()) {
            String line0 = line;
            do {
                line = line0;
                line0 = line.replace(e.getKey(), e.getValue());
            } while (!line0.equals(line));
        }

        return line;
    }

    /** */
    private boolean hasCustomSerdes(TypeMirror type) {
        return TYPE_SERDES.containsKey(className(type))
            || enumType(env, type)
            || isCollection(type)
            || isBiTuple(type)
            || isMap(type)
            || isArray(type)
            || isDto(type);
    }

    /** */
    private boolean isCollection(TypeMirror type) {
        return assignableFrom(env.getTypeUtils().erasure(type), coll);
    }

    /** */
    private boolean isMap(TypeMirror type) {
        return assignableFrom(env.getTypeUtils().erasure(type), map);
    }

    /** */
    private boolean isDto(TypeMirror type) {
        return assignableFrom(type, dtoCls);
    }

    /** */
    private boolean isSerializable(TypeMirror type) {
        return assignableFrom(type, serializableCls);
    }

    /** */
    private static boolean isArray(TypeMirror type) {
        return type.getKind() == TypeKind.ARRAY;
    }

    /** */
    private boolean isBiTuple(TypeMirror type) {
        return env.getTypeUtils().isSameType(env.getTypeUtils().erasure(type), biTuple);
    }

    /** */
    private boolean assignableFrom(TypeMirror type, TypeMirror superType) {
        return env.getTypeUtils().isAssignable(type, superType);
    }

    /** */
    private Stream<String> simpleSerdes(IgniteBiTuple<String, String> serDes, String var, TypeMirror type) {
        return Stream.of((write ? serDes.get1() : ("${var} = " + serDes.get2())) + ";")
            .map(line -> replacePlaceholders(line, var, type));
    }

    /** */
    private static void throwIfNull(TypeMirror type, IgniteBiTuple<String, String> serDes) {
        if (serDes == null)
            throw new IllegalStateException("Unsupported type: " + type);
    }
}
