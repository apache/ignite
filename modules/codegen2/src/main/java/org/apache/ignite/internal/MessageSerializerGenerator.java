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

package org.apache.ignite.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.QualifiedNameable;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.JavaFileObject;

import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.MessageProcessor.MESSAGE_INTERFACE;

/**
 * Generates serializer class for given {@code Message} class. Example of generated code for class {@code XMessage}
 * that specifies single {@code int id} field.
 * <pre>
 * class TestMessageSerializer {
 *     public static boolean writeTo(XMessage msg, ByteBuffer buf, MessageWriter writer) {
 *         writer.setBuffer(buf);
 *
 *         if (!writer.isHeaderWritten()) {
 *             if (!writer.writeHeader(msg.directType()))
 *                 return false;
 *
 *             writer.onHeaderWritten();
 *         }
 *
 *         switch (writer.state()) {
 *             case 0:
 *                 if (!writer.writeInt(msg.id()))
 *                     return false;
 *
 *                 writer.incrementState();
 *         }
 *
 *         return true;
 *     }
 *
 *     public static boolean readFrom(XMessage msg, ByteBuffer buf, MessageReader reader) {
 *          reader.setBuffer(buf);
 *
 *          switch (reader.state()) {
 *              case 0:
 *                  msg.id(reader.readInt());
 *
 *                  if (!reader.isLastRead())
 *                      return false;
 *
 *                  reader.incrementState();
 *          }
 *
 *          return true;
 *     }
 * }
 * </pre>
 */
class MessageSerializerGenerator {
    /** */
    private static final String EMPTY = "";

    /** */
    private static final String TAB = "    ";

    /** */
    private static final String CLS_JAVADOC = "/** \n" +
        " * This class is generated automatically.\n" +
        " *\n" +
        " * @see org.apache.ignite.internal.MessageSerializer\n" +
        " */";

    /** */
    private static final String METHOD_JAVADOC = "/** */";

    /** Collection of lines for {@code writeTo} method. */
    private final Collection<String> write = new ArrayList<>();

    /** Collection of lines for {@code readFrom} method. */
    private final Collection<String> read = new ArrayList<>();

    /** Collection of message-specific imports. */
    private final Set<String> imports = new HashSet<>();

    /** */
    private final ProcessingEnvironment env;

    /** */
    private int indent;

    /** */
    MessageSerializerGenerator(ProcessingEnvironment env) {
        this.env = env;
    }

    /** */
    void generate(TypeElement type, List<VariableElement> fields) throws Exception {
        generateMethods(type, fields);

        String serClsName = type.getSimpleName() + "Serializer";
        String pkgName = env.getElementUtils().getPackageOf(type).getQualifiedName().toString();

        JavaFileObject file = env.getFiler().createSourceFile(pkgName + "." + serClsName);

        try (Writer writer = file.openWriter()) {
            writeClassHeader(writer, pkgName, serClsName);

            // Write #writeTo method.
            for (String w: write)
                writer.write(w + "\n");

            writer.write(TAB + "}\n\n");

            // Write #readFrom method.
            for (String r: read)
                writer.write(r + "\n");

            writer.write(TAB + "}\n");

            writer.write("}");
        }
    }

    /**
     * Generates code for {@code writeTo} and {@code readFrom}.
     */
    private void generateMethods(TypeElement type, List<VariableElement> fields) throws Exception {
        start(type, write, true);
        start(type, read, false);

        indent++;

        int state = 0;

        for (VariableElement field: fields)
            processField(field, state++);

        indent--;

        finish(write);
        finish(read);
    }

    /**
     * Generates start of write/read methods:
     * <pre>
     *     public static boolean writeTo(XMessage msg, ByteBuffer buf, MessageWriter writer) {
     *         writer.setBuffer(buf);
     *
     *         if (!writer.isHeaderWritten()) {
     *             if (!writer.writeHeader(msg.directType()))
     *                 return false;
     *
     *             writer.onHeaderWritten();
     *         }
     * </pre>
     *
     * @param code Code lines.
     * @param write Whether write code is generated.
     */
    private void start(TypeElement type, Collection<String> code, boolean write) {
        indent = 1;

        code.add(line(METHOD_JAVADOC));

        code.add(line("public static boolean %s(%s msg, ByteBuffer buf, %s) {",
            write ? "writeTo" : "readFrom",
            type.getSimpleName().toString(),
            write ? "MessageWriter writer" : "MessageReader reader"));

        indent++;

        code.add(line("%s.setBuffer(buf);", write ? "writer" : "reader"));

        code.add(EMPTY);

        if (write) {
            code.add(line("if (!writer.isHeaderWritten()) {"));

            indent++;

            returnFalseIfWriteFailed(code, "writer.writeHeader", "directType()");

            code.add(EMPTY);
            code.add(line("writer.onHeaderWritten();"));

            indent--;

            code.add(line("}"));
            code.add(EMPTY);
        }

        code.add(line("switch (%s.state()) {", write ? "writer" : "reader"));
    }

    /**
     * @param field Field.
     * @param opt Case option.
     */
    private void processField(VariableElement field, int opt) throws Exception {
        writeField(field, opt);
        readField(field, opt);
    }

    /**
     * Generate code for processing write of single field:
     * <pre>
     * case 0:
     *     if (!writer.writeInt(msg.id()))
     *         return false;
     *
     *     writer.incrementState();
     * </pre>
     *
     * @param field Field.
     * @param opt Case option.
     */
    private void writeField(VariableElement field, int opt) throws Exception {
        write.add(line("case %d:", opt));

        indent++;

        returnFalseIfWriteFailed(field);

        write.add(EMPTY);
        write.add(line("writer.incrementState();"));
        write.add(EMPTY);

        indent--;
    }

    /**
     * Generate code for processing read of single field:
     * <pre>
     * case 0:
     *     msg.id(reader.readInt());
     *
     *     if (!reader.isLastRead())
     *         return false;
     *
     *     reader.incrementState();
     * </pre>
     * @param field Field.
     * @param opt Case option.
     */
    private void readField(VariableElement field, int opt) throws Exception {
        read.add(line("case %d:", opt));

        indent++;

        returnFalseIfReadFailed(field);

        read.add(EMPTY);
        read.add(line("reader.incrementState();"));
        read.add(EMPTY);

        indent--;
    }

    /**
     * Discover access write methods, like {@code writeInt}.
     *
     * @param field Field to generate write code.
     */
    private void returnFalseIfWriteFailed(VariableElement field) throws Exception {
        String getExpr = field.getSimpleName().toString() + "()";
        TypeMirror type = field.asType();

        if (type.getKind().isPrimitive()) {
            String typeName = capitalizeOnlyFirst(type.getKind().name());

            returnFalseIfWriteFailed(write, "writer.write" + typeName, getExpr);

            return;
        }

        if (type.getKind() == TypeKind.ARRAY) {
            ArrayType arrType = (ArrayType)type;
            TypeMirror componentType = arrType.getComponentType();

            if (componentType.getKind().isPrimitive()) {
                String typeName = capitalizeOnlyFirst(componentType.getKind().name());

                returnFalseIfWriteFailed(write, "writer.write" + typeName + "Array", getExpr);

                return;
            }

            imports.add("org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType");

            returnFalseIfWriteFailed(write, "writer.writeObjectArray", getExpr,
                "MessageCollectionItemType." + messageCollectionItemType(componentType));

            return;
        }

        if (type.getKind() == TypeKind.DECLARED) {
            if (sameType(type, String.class))
                returnFalseIfWriteFailed(write, "writer.writeString", getExpr);

            else if (sameType(type, BitSet.class))
                returnFalseIfWriteFailed(write, "writer.writeBitSet", getExpr);

            else if (sameType(type, UUID.class))
                returnFalseIfWriteFailed(write, "writer.writeUuid", getExpr);

            else if (sameType(type, "org.apache.ignite.lang.IgniteUuid"))
                returnFalseIfWriteFailed(write, "writer.writeIgniteUuid", getExpr);

            else if (sameType(type, "org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion"))
                returnFalseIfWriteFailed(write, "writer.writeAffinityTopologyVersion", getExpr);

            else if (assignableFrom(type, type(MESSAGE_INTERFACE)))
                returnFalseIfWriteFailed(write, "writer.writeMessage", getExpr);

            else
                throw new IllegalArgumentException("Unsupported declared type: " + type);

            return;
        }

        throw new IllegalArgumentException("Unsupported type kind: " + type.getKind());
    }

    /**
     * Generate code of writing single field:
     * <pre>
     * if (!writer.writeInt(msg.id()))
     *     return false;
     * </pre>
     */
    private void returnFalseIfWriteFailed(Collection<String> code, String accessor, @Nullable String... args) {
        String argsStr = String.join(",", args);

        code.add(line("if (!%s(msg.%s))", accessor, argsStr));

        indent++;

        code.add(line("return false;"));

        indent--;
    }

    /**
     * Discover access read methods, like {@code readInt}.
     *
     * @param field Field.
     */
    private void returnFalseIfReadFailed(VariableElement field) throws Exception {
        TypeMirror type = field.asType();
        String name = field.getSimpleName().toString();

        if (type.getKind().isPrimitive()) {
            String typeName = capitalizeOnlyFirst(type.getKind().name());

            returnFalseIfReadFailed(name, "reader.read" + typeName);

            return;
        }

        if (type.getKind() == TypeKind.ARRAY) {
            ArrayType arrType = (ArrayType)type;
            TypeMirror componentType = arrType.getComponentType();

            if (componentType.getKind().isPrimitive()) {
                String typeName = capitalizeOnlyFirst(componentType.getKind().name());

                returnFalseIfReadFailed(name, "reader.read" + typeName + "Array");

                return;
            }

            if (componentType.getKind() == TypeKind.ARRAY) {
                TypeMirror ctype = ((ArrayType)componentType).getComponentType();

                assert ctype.getKind().isPrimitive();

                returnFalseIfReadFailed(name, "reader.readObjectArray",
                    "MessageCollectionItemType." + messageCollectionItemType(ctype),
                    ctype.getKind().name().toLowerCase() + "[].class");

                return;
            }

            if (componentType.getKind() == TypeKind.DECLARED) {
                String cls = ((DeclaredType)arrType.getComponentType()).asElement().getSimpleName().toString();

                returnFalseIfReadFailed(name, "reader.readObjectArray",
                    "MessageCollectionItemType." + messageCollectionItemType(componentType),
                    cls + ".class");

                return;
            }
        }

        if (type.getKind() == TypeKind.DECLARED) {
            if (sameType(type, String.class))
                returnFalseIfReadFailed(name, "reader.readString");

            else if (sameType(type, BitSet.class))
                returnFalseIfReadFailed(name, "reader.readBitSet");

            else if (sameType(type, UUID.class))
                returnFalseIfReadFailed(name, "reader.readUuid");

            else if (sameType(type, "org.apache.ignite.lang.IgniteUuid"))
                returnFalseIfReadFailed(name, "reader.readIgniteUuid");

            else if (sameType(type, "org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion"))
                returnFalseIfReadFailed(name, "reader.readAffinityTopologyVersion");

            else if (assignableFrom(type, type(MESSAGE_INTERFACE)))
                returnFalseIfReadFailed(name, "reader.readMessage");

            else
                throw new IllegalArgumentException("Unsupported declared type: " + type);

            return;
        }

        throw new IllegalArgumentException("Unsupported type kind: " + type.getKind());
    }

    /**
     * Find MessageCollectionItemType for a given type.
     * <p>
     * See org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType.
     *
     * @param type Type.
     * @throws Exception in case of not supported type.
     * @return MessageCollectionItemType value.
     */
    private String messageCollectionItemType(TypeMirror type) throws Exception {
        if (type.getKind().isPrimitive())
            return type.getKind().toString();

        if (type.getKind() == TypeKind.ARRAY) {
            ArrayType arrType = (ArrayType)type;
            TypeMirror componentType = arrType.getComponentType();

            if (componentType.getKind().isPrimitive())
                return componentType.getKind().toString() + "_ARR";

            throw new Exception("Do not support non-primitive array type: " + componentType);
        }

        if (type.getKind() == TypeKind.DECLARED) {
            if (sameType(type, String.class))
                return "STRING";

            if (sameType(type, UUID.class))
                return "UUID";

            if (sameType(type, BitSet.class))
                return "BIT_SET";

            if (sameType(type, "org.apache.ignite.lang.IgniteUuid"))
                return "IGNITE_UUID";

            if (sameType(type, "org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion"))
                return "AFFINITY_TOPOLOGY_VERSION";
        }

        if (!assignableFrom(type, type(MESSAGE_INTERFACE)))
            throw new Exception("Do not support type: " + type);

        String cls = ((QualifiedNameable)env.getTypeUtils().asElement(type)).getQualifiedName().toString();

        imports.add(cls);

        return "MSG";
    }

    /**
     * Generate code of reading single field:
     * <pre>
     * msg.id(reader.readInt());
     *
     * if (!reader.isLastRead())
     *     return false;
     * </pre>
     *
     * @param var Variable name.
     * @param mtd Method name.
     */
    private void returnFalseIfReadFailed(String var, String mtd, String... args) {
        String argsStr = String.join(", ", args);

        read.add(line("msg.%s(%s(%s));", var, mtd, argsStr));

        read.add(EMPTY);

        read.add(line("if (!reader.isLastRead())"));

        indent++;

        read.add(line("return false;"));

        indent--;
    }

    /** */
    private void finish(Collection<String> code) {
        code.add(line("}"));
        code.add(EMPTY);

        code.add(line("return true;"));
    }

    /**
     * Creates line with current indent from given arguments.
     *
     * @return Line with current indent.
     */
    private String line(String format, Object... args) {
        SB sb = new SB();

        for (int i = 0; i < indent; i++)
            sb.a(TAB);

        sb.a(String.format(format, args));

        return sb.toString();
    }

    /** Write header of serializer class: license, imports, class declaration. */
    private void writeClassHeader(Writer writer, String pkgName, String serClsName) throws IOException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("license.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            PrintWriter out = new PrintWriter(writer);

            String line;

            while ((line = reader.readLine()) != null)
                out.println(line);
        }

        writer.write("\n");
        writer.write("package " + pkgName + ";\n\n");
        writer.write("import java.nio.ByteBuffer;\n");
        writer.write("import org.apache.ignite.plugin.extensions.communication.MessageWriter;\n");
        writer.write("import org.apache.ignite.plugin.extensions.communication.MessageReader;\n");

        for (String i: imports)
            writer.write("import " + i + ";\n");

        writer.write("\n");
        writer.write(CLS_JAVADOC);
        writer.write("\n");
        writer.write("class " + serClsName + " {\n");
    }

    /** */
    private boolean sameType(TypeMirror type, String typeStr) {
        return env.getTypeUtils().isSameType(type, type(typeStr));
    }

    /** */
    private boolean sameType(TypeMirror type, Class<?> cls) {
        return env.getTypeUtils().isSameType(type, type(cls.getCanonicalName()));
    }

    /** */
    private boolean assignableFrom(TypeMirror type, TypeMirror superType) {
        return env.getTypeUtils().isAssignable(type, superType);
    }

    /** */
    private TypeMirror type(String clazz) {
        Elements elementUtils = env.getElementUtils();

        TypeElement typeElement = elementUtils.getTypeElement(clazz);

        return typeElement != null ? typeElement.asType() : null;
    }

    /** Converts string "BYTE" to string "Byte", with first capital latter. */
    private String capitalizeOnlyFirst(String input) {
        return input.substring(0, 1).toUpperCase() + input.substring(1).toLowerCase();
    }
}
