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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import javax.annotation.processing.FilerException;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.MessageSerializerGenerator.NL;
import static org.apache.ignite.internal.MessageSerializerGenerator.TAB;
import static org.apache.ignite.internal.MessageSerializerGenerator.enumType;
import static org.apache.ignite.internal.MessageSerializerGenerator.identicalFileIsAlreadyGenerated;

/**
 * Generates serializer class for given {@code IgniteDataTransferObject} extension.
 * The generated serializer follows the naming convention:
 * {@code org.apache.ignite.internal.codegen.[IDTOClassName]Serializer}.
 */
public class IDTOSerializerGenerator {
    /** Package for serializers. */
    static final String PKG_NAME = "org.apache.ignite.internal.codegen.idto";

    /** Serializer interface. */
    public static final String DTO_SERDES_INTERFACE = "org.apache.ignite.internal.dto.IgniteDataTransferObjectSerializer";

    /** Class javadoc */
    static final String CLS_JAVADOC = "/** " + NL +
        " * This class is generated automatically." + NL +
        " *" + NL +
        " * @see org.apache.ignite.internal.dto.IgniteDataTransferObject" + NL +
        " */";

    /** Type name to write/read code for the type. */
    private static final Map<String, IgniteBiTuple<String, String>> TYPE_SERDES = new HashMap<>();

    {
        TYPE_SERDES.put(boolean.class.getName(), F.t("out.writeBoolean(obj.${f}());", "obj.${f}(in.readBoolean());"));
        TYPE_SERDES.put(byte.class.getName(), F.t("out.write(obj.${f}());", "obj.${f}(in.read());"));
        TYPE_SERDES.put(short.class.getName(), F.t("out.writeShort(obj.${f}());", "obj.${f}(in.readShort());"));
        TYPE_SERDES.put(int.class.getName(), F.t("out.writeInt(obj.${f}());", "obj.${f}(in.readInt());"));
        TYPE_SERDES.put(long.class.getName(), F.t("out.writeLong(obj.${f}());", "obj.${f}(in.readLong());"));
        TYPE_SERDES.put(float.class.getName(), F.t("out.writeFloat(obj.${f}());", "obj.${f}(in.readFloat());"));
        TYPE_SERDES.put(double.class.getName(), F.t("out.writeDouble(obj.${f}());", "obj.${f}(in.readDouble());"));

        IgniteBiTuple<String, String> objSerdes = F.t("out.writeObject(obj.${f}());", "obj.${f}((${c})in.readObject());");

        TYPE_SERDES.put(Boolean.class.getName(), objSerdes);
        TYPE_SERDES.put(Byte.class.getName(), objSerdes);
        TYPE_SERDES.put(Short.class.getName(), objSerdes);
        TYPE_SERDES.put(Integer.class.getName(), objSerdes);
        TYPE_SERDES.put(Long.class.getName(), objSerdes);
        TYPE_SERDES.put(Float.class.getName(), objSerdes);
        TYPE_SERDES.put(Double.class.getName(), objSerdes);

        TYPE_SERDES.put(String.class.getName(), F.t("U.writeString(out, obj.${f}());", "obj.${f}(U.readString(in));"));
        TYPE_SERDES.put(UUID.class.getName(), F.t("U.writeUuid(out, obj.${f}());", "obj.${f}(U.readUuid(in));"));
        TYPE_SERDES.put("org.apache.ignite.lang.IgniteUuid", F.t("U.writeIgniteUuid(out, obj.${f}());", "obj.${f}(U.readIgniteUuid(in));"));
        TYPE_SERDES.put("org.apache.ignite.internal.processors.cache.version.GridCacheVersion", objSerdes);

        TYPE_SERDES.put(Map.class.getName(), F.t("U.writeMap(out, obj.${f}());", "obj.${f}(U.readMap(in));"));
    }

    /** Write/Read code for enum. */
    private static final IgniteBiTuple<String, String> ENUM_SERDES =
        F.t("U.writeEnum(out, obj.${f}());", "obj.${f}(U.readEnum(in, ${c}.class));");

    /** Write/Read code for array. */
    private static final IgniteBiTuple<String, String> OBJ_ARRAY_SERDES =
        F.t("U.writeArray(out, obj.${f}());", "obj.${f}(U.readArray(in, ${c}.class));");

    /** Type name to write/read code for the array of type. */
    private static final Map<String, IgniteBiTuple<String, String>> ARRAY_TYPE_SERDES = new HashMap<>();

    {
        ARRAY_TYPE_SERDES.put(byte.class.getName(), F.t("U.writeByteArray(out, obj.${f}());", "obj.${f}(U.readByteArray(in));"));
        ARRAY_TYPE_SERDES.put(int.class.getName(), F.t("U.writeIntArray(out, obj.${f}());", "obj.${f}(U.readIntArray(in));"));
        ARRAY_TYPE_SERDES.put(long.class.getName(), F.t("U.writeLongArray(out, obj.${f}());", "obj.${f}(U.readLongArray(in));"));
        ARRAY_TYPE_SERDES.put(String.class.getName(), OBJ_ARRAY_SERDES);
        ARRAY_TYPE_SERDES.put(UUID.class.getName(), OBJ_ARRAY_SERDES);
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
        return PKG_NAME + "." + serializerName();
    }

    /**
     * @return {@code True} if generation succeed.
     * @throws Exception in case of error.
     */
    public boolean generate() throws Exception {
        String serClsName = type.getSimpleName() + "Serializer";
        String serCode = generateSerializerCode();

        try {
            JavaFileObject file = env.getFiler().createSourceFile(PKG_NAME + "." + serClsName);

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
            if (!identicalFileIsAlreadyGenerated(env, serCode, PKG_NAME, serClsName)) {
                env.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    serClsName + " is already generated. Try 'mvn clean install' to fix the issue.");

                throw e;
            }

            return false;
        }
    }

    /** @return Code for the calss implementing {@code org.apache.ignite.internal.dto.IgniteDataTransferObjectSerializer}. */
    private String generateSerializerCode() throws IOException {
        imports.add("org.apache.ignite.internal.dto.IgniteDataTransferObjectSerializer");
        imports.add(type.getQualifiedName().toString());
        imports.add(ObjectOutput.class.getName());
        imports.add(ObjectInput.class.getName());
        imports.add(IOException.class.getName());
        imports.add("org.apache.ignite.internal.util.typedef.internal.U");

        String simpleClsName = String.valueOf(type.getSimpleName());

        List<VariableElement> flds = fields(type);

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
     * @throws IOException
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
        writer.write("package " + PKG_NAME + ";" + NL + NL);

        for (String regularImport: imports)
            writer.write("import " + regularImport + ";" + NL);

        writer.write(NL);
        writer.write(CLS_JAVADOC);
        writer.write(NL);
        writer.write("public class " + serializerName() + " implements " + simpleName(DTO_SERDES_INTERFACE) + "<" + simpleClsName + "> {" + NL);
    }

    /** @return Lines for generated {@code IgniteDataTransferObjectSerializer#writeExternal(T, ObjectOutput)} method. */
    private List<String> generateWrite(String clsName, List<VariableElement> flds) {
        List<String> code = new ArrayList<>();

        code.add("/** {@inheritDoc} */");
        code.add("@Override public void writeExternal(" + clsName + " obj, ObjectOutput out) throws IOException {");

        fieldsSerdes(flds, IgniteBiTuple::get1).forEach(line -> code.add(TAB + line));

        code.add("}");

        return code;
    }

    /** @return Lines for generated {@code IgniteDataTransferObjectSerializer#readExternal(T, ObjectInput)} method. */
    private List<String> generateRead(String clsName, List<VariableElement> flds) {
        List<String> code = new ArrayList<>();

        code.add("/** {@inheritDoc} */");
        code.add("@Override public void readExternal(" + clsName + " obj, ObjectInput in) throws IOException, ClassNotFoundException {");
        fieldsSerdes(flds, IgniteBiTuple::get2).forEach(line -> code.add(TAB + line));
        code.add("}");

        return code;
    }

    /**
     * @param flds Fields to generated serdes for.
     * @param lineProvider Function to generated serdes code for the field.
     * @return Lines to serdes fields.
     */
    private List<String> fieldsSerdes(
        List<VariableElement> flds,
        Function<IgniteBiTuple<String, String>, String> lineProvider
    ) {
        List<String> code = new ArrayList<>();

        for (VariableElement fld : flds) {
            TypeMirror type = fld.asType();
            TypeMirror comp = null;

            IgniteBiTuple<String, String> serDes;

            if (type.getKind() == TypeKind.ARRAY) {
                comp = ((ArrayType)type).getComponentType();

                serDes = enumType(env, comp) ? OBJ_ARRAY_SERDES : ARRAY_TYPE_SERDES.get(className(comp));
            }
            else
                serDes = enumType(env, type) ? ENUM_SERDES : TYPE_SERDES.get(className(type));

            if (serDes != null) {
                String pattern = lineProvider.apply(serDes);

                code.add(pattern
                    .replaceAll("\\$\\{f}", fld.getSimpleName().toString())
                    .replaceAll("\\$\\{c}", simpleClassName(comp == null ? type : comp)));
            }
            else
                throw new IllegalStateException("Unsupported type: " + type);
        }

        return code;
    }

    /** @return List of non-static and non-transient field for given {@code type}. */
    private List<VariableElement> fields(TypeElement type) {
        List<VariableElement> res = new ArrayList<>();

        while (type != null) {
            for (Element el: type.getEnclosedElements()) {
                if (el.getKind() != ElementKind.FIELD)
                    continue;

                if (el.getModifiers().contains(Modifier.STATIC) || el.getModifiers().contains(Modifier.TRANSIENT))
                    continue;

                res.add((VariableElement)el);
            }

            Element superType = env.getTypeUtils().asElement(type.getSuperclass());

            type = (TypeElement)superType;
        }

        return res;
    }

    /** @return FQN of {@code comp}. */
    private static String className(TypeMirror comp) {
        String n = comp.toString();
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

        if (!fqn.startsWith("java.lang"))
            imports.add(fqn);

        return simpleName(fqn);
    }

    /** @return Simple class name. */
    public static String simpleName(String fqn) {
        return fqn.substring(fqn.lastIndexOf('.') + 1);
    }
}
