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
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.processing.FilerException;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.QualifiedNameable;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.MessageProcessor.MESSAGE_INTERFACE;

/**
 * Generates serializer class for given {@code Message} class. The generated serializer follows the naming convention:
 * {@code org.apache.ignite.internal.codegen.[MessageClassName]Serializer}.
 */
class MessageSerializerGenerator {
    /** */
    private static final String EMPTY = "";

    /** */
    private static final String TAB = "    ";

    /** */
    private static final String NL = System.lineSeparator();

    /** */
    private static final String PKG_NAME = "org.apache.ignite.internal.codegen";

    /** */
    private static final String CLS_JAVADOC = "/** " + NL +
        " * This class is generated automatically." + NL +
        " *" + NL +
        " * @see org.apache.ignite.internal.MessageProcessor" + NL +
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

        imports.add(type.getQualifiedName().toString());

        String serClsName = type.getSimpleName() + "Serializer";
        String serCode = generateSerializerCode(serClsName);

        try {
            JavaFileObject file = env.getFiler().createSourceFile(PKG_NAME + "." + serClsName);

            try (Writer writer = file.openWriter()) {
                writer.append(serCode);
                writer.flush();
            }
        }
        catch (FilerException e) {
            // IntelliJ IDEA parses Ignite's pom.xml and configures itself to use this annotation processor on each Run.
            // During a Run, it invokes the processor and may fail when attempting to generate sources that already exist.
            // There is no a setting to disable this invocation. The IntelliJ community suggests a workaround — delegating
            // all Run commands to Maven. However, this significantly slows down test startup time.
            // This hack checks whether the content of a generating file is identical to already existed file, and skips
            // handling this class if it is.
            if (!identicalFileIsAlreadyGenerated(serCode, serClsName)) {
                env.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "MessageSerializer " + serClsName + " is already generated. Try 'mvn clean install' to fix the issue.");

                throw e;
            }
        }
    }

    /** Generates full code for a serializer class. */
    private String generateSerializerCode(String serClsName) throws IOException {
        try (Writer writer = new StringWriter()) {
            writeClassHeader(writer, PKG_NAME, serClsName);

            // Write #writeTo method.
            for (String w: write)
                writer.write(w + NL);

            writer.write(TAB + "}" + NL + NL);

            // Write #readFrom method.
            for (String r: read)
                writer.write(r + NL);

            writer.write(TAB + "}" + NL);

            writer.write("}");

            return writer.toString();
        }
    }

    /** Generates code for {@code writeTo} and {@code readFrom}. */
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
     *     public boolean writeTo(Message m, ByteBuffer buf, MessageWriter writer) {
     *         TestMessage msg = (TestMessage)m;
     *
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

        code.add(line("@Override public boolean %s(Message m, ByteBuffer buf, %s) {",
            write ? "writeTo" : "readFrom",
            write ? "MessageWriter writer" : "MessageReader reader"));

        indent++;

        code.add(line("%s msg = (%s)m;", type.getSimpleName().toString(), type.getSimpleName().toString()));
        code.add(EMPTY);
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
        String methodName = field.getAnnotation(Order.class).method();

        String getExpr = (F.isEmpty(methodName) ? field.getSimpleName().toString() : methodName) + "()";

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

            else if (assignableFrom(erasedType(type), type(Map.class.getName()))) {
                List<? extends TypeMirror> typeArgs = ((DeclaredType)type).getTypeArguments();

                assert typeArgs.size() == 2;

                imports.add("org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType");

                returnFalseIfWriteFailed(write, "writer.writeMap", getExpr,
                    "MessageCollectionItemType." + messageCollectionItemType(typeArgs.get(0)),
                    "MessageCollectionItemType." + messageCollectionItemType(typeArgs.get(1)));
            }

            else if (assignableFrom(type, type(MESSAGE_INTERFACE)))
                returnFalseIfWriteFailed(write, "writer.writeMessage", getExpr);

            else if (assignableFrom(erasedType(type), type(Collection.class.getName()))) {
                List<? extends TypeMirror> typeArgs = ((DeclaredType)type).getTypeArguments();

                assert typeArgs.size() == 1;

                imports.add("org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType");

                returnFalseIfWriteFailed(write, "writer.writeCollection", getExpr,
                    "MessageCollectionItemType." + messageCollectionItemType(typeArgs.get(0)));
            }

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

        String methodName = field.getAnnotation(Order.class).method();

        String name = F.isEmpty(methodName) ? field.getSimpleName().toString() : methodName;

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

            else if (assignableFrom(erasedType(type), type(Map.class.getName()))) {
                List<? extends TypeMirror> typeArgs = ((DeclaredType)type).getTypeArguments();

                assert typeArgs.size() == 2;

                boolean linked = sameErasedType(type, LinkedHashMap.class);

                returnFalseIfReadFailed(name, "reader.readMap",
                    "MessageCollectionItemType." + messageCollectionItemType(typeArgs.get(0)),
                    "MessageCollectionItemType." + messageCollectionItemType(typeArgs.get(1)),
                    linked ? "true" : "false");
            }

            else if (assignableFrom(type, type(MESSAGE_INTERFACE)))
                returnFalseIfReadFailed(name, "reader.readMessage");

            else if (assignableFrom(erasedType(type), type(Collection.class.getName()))) {
                List<? extends TypeMirror> typeArgs = ((DeclaredType)type).getTypeArguments();

                assert typeArgs.size() == 1;

                returnFalseIfReadFailed(name, "reader.readCollection",
                    "MessageCollectionItemType." + messageCollectionItemType(typeArgs.get(0)));
            }

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

            PrimitiveType primitiveType = unboxedType(type);

            if (primitiveType != null)
                return primitiveType.getKind().toString();
        }

        if (!assignableFrom(type, type(MESSAGE_INTERFACE)))
            throw new Exception("Do not support type: " + type);

        String cls = ((QualifiedNameable)env.getTypeUtils().asElement(type)).getQualifiedName().toString();

        imports.add(cls);

        return "MSG";
    }

    /**
     * Returns the type (a primitive type) of unboxed values of a given type.
     * That is, <i>unboxing conversion</i> is applied.
     *
     * @param type  the type to be unboxed
     * @return the type of unboxed value of type {@code type} or null if the given type has no unboxing conversion
     */
    private PrimitiveType unboxedType(TypeMirror type) {
        try {
            return env.getTypeUtils().unboxedType(type);
        }
        catch (IllegalArgumentException e) {
            return null;
        }
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

        writer.write(NL);
        writer.write("package " + pkgName + ";" + NL + NL);
        writer.write("import java.nio.ByteBuffer;" + NL);
        writer.write("import org.apache.ignite.plugin.extensions.communication.Message;" + NL);
        writer.write("import org.apache.ignite.plugin.extensions.communication.MessageSerializer;" + NL);
        writer.write("import org.apache.ignite.plugin.extensions.communication.MessageWriter;" + NL);
        writer.write("import org.apache.ignite.plugin.extensions.communication.MessageReader;" + NL);

        for (String i: imports)
            writer.write("import " + i + ";" + NL);

        writer.write(NL);
        writer.write(CLS_JAVADOC);
        writer.write(NL);
        writer.write("public class " + serClsName + " implements MessageSerializer {" + NL);
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
    private boolean sameErasedType(TypeMirror type, Class<?> cls) {
        return env.getTypeUtils().isSameType(erasedType(type), erasedType(type(cls.getCanonicalName())));
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

    /** */
    private TypeMirror erasedType(TypeMirror type) {
        return env.getTypeUtils().erasure(type);
    }

    /** Converts string "BYTE" to string "Byte", with first capital latter. */
    private String capitalizeOnlyFirst(String input) {
        return input.substring(0, 1).toUpperCase() + input.substring(1).toLowerCase();
    }

    /** @return {@code true} if trying to generate file with the same content. */
    private boolean identicalFileIsAlreadyGenerated(String srcCode, String clsName) {
        try {
            String fileName = PKG_NAME.replace('.', '/') + '/' + clsName + ".java";
            FileObject prevFile = env.getFiler().getResource(StandardLocation.SOURCE_OUTPUT, "", fileName);

            String prevFileContent;
            try (Reader r = prevFile.openReader(true)) {
                prevFileContent = content(r);
            }

            // We are ok, for some reason the same file is already generated (Intellij IDEA might do it).
            if (prevFileContent.contentEquals(srcCode))
                return true;
        }
        catch (Exception ignoredAttemptToGetExistingFile) {
            // We have some other problem, not an existing file.
        }

        return false;
    }

    /** */
    private String content(Reader reader) throws IOException {
        BufferedReader br = new BufferedReader(reader);
        StringBuilder sb = new StringBuilder();
        String line;

        while ((line = br.readLine()) != null)
            sb.append(line).append(NL);

        // Delete last line separator.
        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
    }
}
