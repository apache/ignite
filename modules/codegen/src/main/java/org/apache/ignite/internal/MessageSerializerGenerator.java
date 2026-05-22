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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import javax.annotation.processing.FilerException;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.QualifiedNameable;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import org.apache.ignite.internal.systemview.SystemViewRowAttributeWalkerProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.MessageProcessor.COMPRESSED_MESSAGE_INTERFACE;
import static org.apache.ignite.internal.MessageProcessor.MARSHALLABLE_MESSAGE_INTERFACE;
import static org.apache.ignite.internal.MessageProcessor.MESSAGE_INTERFACE;

/**
 * Generates serializer class for given {@code Message} class. The generated serializer follows the naming convention:
 * {@code org.apache.ignite.internal.codegen.[MessageClassName]Serializer}.
 */
public class MessageSerializerGenerator {
    /** */
    private static final String EMPTY = "";

    /** */
    public static final String TAB = "    ";

    /** */
    public static final String NL = System.lineSeparator();

    /** */
    private static final String CLS_JAVADOC = "/**" + NL +
        " * This class is generated automatically." + NL +
        " *" + NL +
        " * @see org.apache.ignite.internal.MessageProcessor" + NL +
        " */";

    /** */
    public static final String METHOD_JAVADOC = "/** */";

    /** */
    private static final String RETURN_FALSE_STMT = "return false;";

    /** */
    static final String DLFT_ENUM_MAPPER_CLS = "org.apache.ignite.plugin.extensions.communication.mappers.DefaultEnumMapper";

    /** */
    private static final String COMPRESSED_MSG_ERROR = "CompressedMessage should not be used explicitly. " +
        "To compress the required field use the @Compress annotation.";

    /** Collection of lines for {@code writeTo} method. */
    private final List<String> write = new ArrayList<>();

    /** Collection of lines for {@code readFrom} method. */
    private final List<String> read = new ArrayList<>();

    /** */
    private final List<String> marshall = new ArrayList<>();

    /** Collection of message-specific imports. */
    private final Set<String> imports = new TreeSet<>();

    /** Collection of Serializer class fields containing mappers for message enum fields. */
    private final Set<String> fields = new TreeSet<>();

    /** */
    private final ProcessingEnvironment env;

    /** Stored type of the message being processed. */
    private TypeElement type;

    /** The marshallable message type. */
    private final TypeMirror marshallableMsgType;

    /** */
    private int indent;

    /** */
    MessageSerializerGenerator(ProcessingEnvironment env) {
        this.env = env;

        marshallableMsgType = env.getElementUtils().getTypeElement(MARSHALLABLE_MESSAGE_INTERFACE).asType();
    }

    /** */
    void generate(TypeElement type, List<VariableElement> fields) throws Exception {
        assert this.type == null : "Message serializer generator isn't stateless and is supposed to be single-use.";

        this.type = type;

        generateMethods(fields);

        SystemViewRowAttributeWalkerProcessor.superclasses(env, type).forEach(el -> imports.add(el.toString()));

        String serClsName = type.getSimpleName() + (marshallableMessage() ? "Marshallable" : "") + "Serializer";
        String serFqnClsName = env.getElementUtils().getPackageOf(type) + "." + serClsName;
        String serCode = generateSerializerCode(serClsName);

        try {
            JavaFileObject file = env.getFiler().createSourceFile(serFqnClsName);

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
            if (!identicalFileIsAlreadyGenerated(env, serCode, serFqnClsName)) {
                env.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "MessageSerializer " + serClsName + " is already generated. Try 'mvn clean install' to fix the issue.");

                throw e;
            }
        }
    }

    /** Generates full code for a serializer class. */
    private String generateSerializerCode(String serClsName) throws IOException {
        if (marshallableMessage()) {
            fields.add("private final Marshaller marshaller;");
            fields.add("private final ClassLoader clsLdr;");
        }

        try (Writer writer = new StringWriter()) {
            writeClassHeader(writer, env.getElementUtils().getPackageOf(type).toString(), serClsName);

            writeClassFields(writer);

            writeConstructor(writer, serClsName);

            // Write #writeTo method.
            for (String w: write)
                writer.write(w + NL);

            writer.write(TAB + "}" + NL + NL);

            // Write #readFrom method.
            for (String r: read)
                writer.write(r + NL);

            writer.write(TAB + "}" + NL);

            if (!marshall.isEmpty()) {
                writer.write(NL);

                for (String p: marshall)
                    writer.write(p + NL);
            }

            writer.write("}");

            return writer.toString();
        }
    }

    /** */
    private void writeConstructor(Writer writer, String serClsName) throws IOException {
        if (!marshallableMessage())
            return;

        ++indent;

        writer.write(indentedLine(METHOD_JAVADOC));
        writer.write(NL);
        writer.write(indentedLine("public " + serClsName + "(Marshaller marshaller, ClassLoader clsLdr) {"));

        writer.write(NL);

        ++indent;

        writer.write(indentedLine("this.marshaller = marshaller;"));
        writer.write(NL);
        writer.write(indentedLine("this.clsLdr = clsLdr;"));

        --indent;

        writer.write(NL);

        writer.write(indentedLine("}"));
        writer.write(NL);

        --indent;
    }

    /** Generates code for {@code writeTo} and {@code readFrom}. */
    private void generateMethods(List<VariableElement> fields) throws Exception {
        start(write, true);
        start(read, false);

        indent++;

        int state = 0;

        for (VariableElement field: fields)
            processField(field, state++);

        indent--;

        finish(write, false, false);
        finish(read, true, marshallableMessage());

        generateMarshallMethods(fields);
    }

    /** */
    private void generateMarshallMethods(List<VariableElement> orderedFields) throws Exception {
        imports.add("org.apache.ignite.IgniteCheckedException");
        imports.add("org.apache.ignite.internal.processors.cache.CacheObjectValueContext");
        imports.add("org.apache.ignite.internal.processors.cache.GridCacheSharedContext");
        imports.add("org.apache.ignite.internal.processors.cache.GridCacheContext");

        indent = 1;

        marshall.add(indentedLine(METHOD_JAVADOC));

        marshall.add(indentedLine(
            "@Override public void prepareMarshal(" + simpleNameWithGeneric(type) +
                " msg, GridCacheSharedContext<?,?> sctx, GridCacheContext<?, ?> nested) throws IgniteCheckedException {"));

        indent++;

        if (isCacheIdAwareMessage(type))
            marshall.add(
                indentedLine("GridCacheContext<?, ?> ctx = nested == null ? sctx.cacheContext(msg.cacheId()) : nested;"));
        else
            marshall.add(indentedLine("GridCacheContext<?, ?> ctx = nested;"));

        if (marshallableMessage()) {
            marshall.add(EMPTY);

            marshall.add(indentedLine("msg.prepareMarshal(marshaller);"));
        }

        for (VariableElement field : orderedFields) {
            List<String> marshalled = marshall(field.asType(), fieldAccessor(field));

            if (!marshalled.isEmpty()) {
                if (!marshall.get(marshall.size() - 1).equals(EMPTY))
                    marshall.add(EMPTY);

                marshall.addAll(marshalled);
            }
        }

        indent--;

        marshall.add(indentedLine("}"));
    }

    /** */
    private List<String> marshall(TypeMirror t, String accessor) {
        if (t.getKind() == TypeKind.ARRAY) {
            TypeMirror comp = ((ArrayType)t).getComponentType();

            if (comp.getKind() == TypeKind.DECLARED) {
                List<String> code = new ArrayList<>();

                imports.add(((QualifiedNameable)((DeclaredType)comp).asElement()).getQualifiedName().toString());

                code.add(indentedLine("if (%s != null) {", accessor));

                indent++;

                String el = "e" + indent;

                code.add(indentedLine("for (%s %s : %s) {", ((DeclaredType)comp).asElement().getSimpleName().toString(), el, accessor));

                indent++;

                List<String> res = marshall(comp, el);

                code.addAll(res);

                indent--;

                code.add(indentedLine("}"));

                indent--;

                code.add(indentedLine("}"));

                if (res.isEmpty())
                    return Collections.emptyList();
                else
                    return code;
            }
        }
        else if (t.getKind() == TypeKind.DECLARED || t.getKind() == TypeKind.TYPEVAR) {
            if (isMessage(t)) {
                List<String> code = new ArrayList<>();

                code.add(indentedLine("if (%s != null)", accessor));

                indent++;

                code.add(indentedLine(
                    "sctx.gridIO().messageFactory().serializer(%s.directType()).prepareMarshal(%s, sctx, ctx);",
                    accessor,
                    accessor));

                indent--;

                return code;
            }
            else if (isCacheObject(t)) {
                List<String> code = new ArrayList<>();

                code.add(indentedLine("if (%s != null)", accessor));

                indent++;

                code.add(indentedLine("%s.prepareMarshal(ctx != null ? ctx.cacheObjectContext() : null);", accessor));

                indent--;

                return code;
            }
            else if (assignableFrom(erasedType(t), type(Map.class.getName()))) {
                List<? extends TypeMirror> args = ((DeclaredType)t).getTypeArguments();

                TypeMirror keyType = args.get(0);
                TypeMirror valType = args.get(1);

                List<String> code = new ArrayList<>();

                code.add(indentedLine("if (%s != null) {", accessor));

                indent++;

                String el = "e" + indent;

                indent++; // Emulating subsequent indent.
                List<String> keyRes = marshall(keyType, el);
                List<String> valRes = marshall(valType, el);
                indent--;

                if (!keyRes.isEmpty() && (keyType.getKind() == TypeKind.DECLARED || keyType.getKind() == TypeKind.TYPEVAR)) {
                    Element elem = element(keyType);
                    
                    imports.add(((QualifiedNameable)(elem)).getQualifiedName().toString());
                    imports.add("java.util.Collection");

                    String type = elem.getSimpleName().toString();

                    code.add(indentedLine("for (%s %s : ((Collection<? extends %s>)%s.keySet())) {", type, el, type, accessor));

                    indent++;

                    code.addAll(keyRes);

                    indent--;

                    code.add(indentedLine("}"));
                }

                if (!valRes.isEmpty() && (valType.getKind() == TypeKind.DECLARED || valType.getKind() == TypeKind.TYPEVAR)) {
                    Element elem = element(valType);
                    
                    imports.add(((QualifiedNameable)(elem)).getQualifiedName().toString());
                    imports.add("java.util.Collection");                    

                    String type = elem.getSimpleName().toString();

                    code.add(indentedLine("for (%s %s : ((Collection<? extends %s>)%s.values())) {", type, el, type, accessor));

                    indent++;

                    code.addAll(valRes);

                    indent--;

                    code.add(indentedLine("}"));
                }

                indent--;

                code.add(indentedLine("}"));

                if (keyRes.isEmpty() && valRes.isEmpty())
                    return Collections.emptyList();
                else
                    return code;
            }
            else if (assignableFrom(erasedType(t), type(Collection.class.getName()))) {
                List<? extends TypeMirror> args = ((DeclaredType)t).getTypeArguments();

                TypeMirror arg = args.get(0);

                if ((arg.getKind() == TypeKind.DECLARED || arg.getKind() == TypeKind.TYPEVAR)) {
                    List<String> code = new ArrayList<>();

                    Element elem = element(arg);

                    imports.add(((QualifiedNameable)(elem)).getQualifiedName().toString());
                    imports.add("java.util.Collection");

                    String el = "e" + indent;

                    code.add(indentedLine("if (%s != null) {", accessor));

                    indent++;

                    String type = elem.getSimpleName().toString();

                    code.add(indentedLine("for (%s %s : (Collection<? extends %s>)%s) {", type, el, type, accessor));

                    indent++;

                    List<String> res = marshall(arg, el);

                    code.addAll(res);

                    indent--;

                    code.add(indentedLine("}"));

                    indent--;

                    code.add(indentedLine("}"));

                    if (res.isEmpty())
                        return Collections.emptyList();
                    else
                        return code;
                }
            }
        }

        return Collections.emptyList();
    }

    /** */
    private boolean isCacheObject(TypeMirror type) {
        return assignableFrom(type, type("org.apache.ignite.internal.processors.cache.CacheObject"));
    }

    /** */
    private boolean isMessage(TypeMirror type) {
        return assignableFrom(type, type(MESSAGE_INTERFACE));
    }

    /** True if {@code te} extends {@code CacheIdAware} and therefore carries its own per-cache {@code cacheId()}. */
    private boolean isCacheIdAwareMessage(TypeElement te) {
        return assignableFrom(te.asType(), type("org.apache.ignite.plugin.extensions.communication.CacheIdAware"));
    }

    /** */
    private String fieldAccessor(VariableElement field) {
        String name = field.getSimpleName().toString();

        return "msg." + name;
    }
    
    /** */
    private Element element(TypeMirror type) {
        return type.getKind() == TypeKind.DECLARED ?
            ((DeclaredType)type).asElement() :
            ((DeclaredType)((TypeVariable)type).getUpperBound()).asElement();
    }

    /**
     * Generates start of write/read methods:
     * <pre>
     *     public boolean writeTo(Message m, MessageWriter writer) {
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
    private void start(Collection<String> code, boolean write) {
        indent = 1;

        code.add(indentedLine(METHOD_JAVADOC));

        code.add(indentedLine("@Override public boolean %s(" + simpleNameWithGeneric(type) + " msg, %s) {",
            write ? "writeTo" : "readFrom",
            write ? "MessageWriter writer" : "MessageReader reader"));

        indent++;

        if (write) {
            code.add(indentedLine("if (!writer.isHeaderWritten()) {"));

            indent++;

            returnFalseIfWriteFailed(code, "writer.writeHeader", "directType()");

            code.add(EMPTY);
            code.add(indentedLine("writer.onHeaderWritten();"));

            indent--;

            code.add(indentedLine("}"));
            code.add(EMPTY);
        }

        code.add(indentedLine("switch (%s.state()) {", write ? "writer" : "reader"));
    }

    /**
     * @param field Field.
     * @param opt Case option.
     */
    private void processField(VariableElement field, int opt) throws Exception {
        if (assignableFrom(field.asType(), type(Throwable.class.getName())))
            throw new UnsupportedOperationException("You should use ErrorMessage for serialization of throwables.");

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
        write.add(indentedLine("case %d:", opt));

        indent++;

        returnFalseIfWriteFailed(field);

        write.add(EMPTY);
        write.add(indentedLine("writer.incrementState();"));
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
        read.add(indentedLine("case %d:", opt));

        indent++;

        returnFalseIfReadFailed(field);

        read.add(EMPTY);
        read.add(indentedLine("reader.incrementState();"));
        read.add(EMPTY);

        indent--;
    }

    /**
     * Discover access write methods, like {@code writeInt}.
     *
     * @param field Field to generate write code.
     */
    private void returnFalseIfWriteFailed(VariableElement field) throws Exception {
        String getExpr = field.getSimpleName().toString();

        TypeMirror type = field.asType();

        boolean compress = field.getAnnotation(Compress.class) != null;

        if (compress)
            checkTypeForCompress(type);

        if (type.getKind().isPrimitive()) {
            String typeName = capitalizeOnlyFirst(type.getKind().name());

            returnFalseIfWriteFailed(write, field, "writer.write" + typeName, getExpr);

            return;
        }

        if (type.getKind() == TypeKind.ARRAY) {
            ArrayType arrType = (ArrayType)type;
            TypeMirror componentType = arrType.getComponentType();

            if (componentType.getKind().isPrimitive()) {
                String typeName = capitalizeOnlyFirst(componentType.getKind().name());

                returnFalseIfWriteFailed(write, field, "writer.write" + typeName + "Array", getExpr);

                return;
            }

            returnFalseIfWriteFailed(write, field, "writer.writeObjectArray", getExpr, messageCollectionItemTypes(field, type));

            return;
        }

        if (type.getKind() == TypeKind.DECLARED) {
            if (sameType(type, String.class))
                returnFalseIfWriteFailed(write, field, "writer.writeString", getExpr);

            else if (sameType(type, BitSet.class))
                returnFalseIfWriteFailed(write, field, "writer.writeBitSet", getExpr);

            else if (sameType(type, UUID.class))
                returnFalseIfWriteFailed(write, field, "writer.writeUuid", getExpr);

            else if (sameType(type, "org.apache.ignite.lang.IgniteUuid"))
                returnFalseIfWriteFailed(write, field, "writer.writeIgniteUuid", getExpr);

            else if (sameType(type, "org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion"))
                returnFalseIfWriteFailed(write, field, "writer.writeAffinityTopologyVersion", getExpr);

            else if (assignableFrom(erasedType(type), type(Map.class.getName()))) {
                List<String> args = new ArrayList<>();

                args.add(getExpr);
                args.add(messageCollectionItemTypes(field, type));

                if (compress)
                    args.add("true"); // the value of the compress argument in the MessageWriter#writeMap method

                returnFalseIfWriteFailed(write, field, "writer.writeMap", args.toArray(String[]::new));
            }

            else if (assignableFrom(type, type("org.apache.ignite.internal.processors.cache.KeyCacheObject")))
                returnFalseIfWriteFailed(write, field, "writer.writeKeyCacheObject", getExpr);

            else if (assignableFrom(type, type("org.apache.ignite.internal.processors.cache.CacheObject")))
                returnFalseIfWriteFailed(write, field, "writer.writeCacheObject", getExpr);

            else if (assignableFrom(type, type("org.apache.ignite.internal.util.GridLongList")))
                returnFalseIfWriteFailed(write, field, "writer.writeGridLongList", getExpr);

            else if (assignableFrom(type, type(MESSAGE_INTERFACE))) {
                if (sameType(type, COMPRESSED_MESSAGE_INTERFACE))
                    throw new IllegalArgumentException(COMPRESSED_MSG_ERROR);

                if (compress)
                    returnFalseIfWriteFailed(write, field, "writer.writeMessage", getExpr, "true");
                else
                    returnFalseIfWriteFailed(write, field, "writer.writeMessage", getExpr);
            }

            else if (assignableFrom(erasedType(type), type(Collection.class.getName())))
                returnFalseIfWriteFailed(write, field, "writer.writeCollection", getExpr, messageCollectionItemTypes(field, type));

            else if (enumType(env, type)) {
                Element element = env.getTypeUtils().asElement(type);
                imports.add(element.toString());

                String enumName = element.getSimpleName().toString();
                String enumFieldPrefix = typeNameToFieldName(enumName);

                String mapperCallStmnt;

                CustomMapper custMapperAnn = field.getAnnotation(CustomMapper.class);

                if (custMapperAnn != null) {
                    String fullMapperName = custMapperAnn.value();
                    if (fullMapperName == null || fullMapperName.isEmpty())
                        throw new IllegalArgumentException("Please specify a not-null not-empty EnumMapper class name");

                    imports.add("org.apache.ignite.plugin.extensions.communication.mappers.EnumMapper");
                    imports.add(fullMapperName);

                    String simpleName = fullMapperName.substring(fullMapperName.lastIndexOf('.') + 1);

                    String mapperFieldName = enumFieldPrefix + "Mapper";

                    fields.add("private final EnumMapper<" + enumName + "> " + mapperFieldName + " = new " + simpleName + "();");

                    mapperCallStmnt = mapperFieldName + ".encode";
                }
                else {
                    imports.add(DLFT_ENUM_MAPPER_CLS);
                    String enumValuesFieldName = enumFieldPrefix + "Vals";

                    fields.add("private final " + enumName + "[] " + enumValuesFieldName + " = " + enumName + ".values();");

                    mapperCallStmnt = "DefaultEnumMapper.INSTANCE.encode";
                }

                returnFalseIfEnumWriteFailed(write, field, "writer.writeByte", mapperCallStmnt, getExpr);
            }
            else
                throw new IllegalArgumentException("Unsupported declared type: " + type);

            return;
        }

        throw new IllegalArgumentException("Unsupported type kind: " + type.getKind());
    }

    /**
     * Converts type name to camel case field name. Example: {@code "MyType"} -> {@code "myType"}.
     */
    private String typeNameToFieldName(String typeName) {
        char[] typeNameChars = typeName.toCharArray();
        typeNameChars[0] = Character.toLowerCase(typeNameChars[0]);
        return new String(typeNameChars);
    }

    /**
     * Generate code of writing header.
     * <pre>
     * if (!writer.writeHeader(msg.directType()))
     *     return false;
     * </pre>
     */
    private void returnFalseIfWriteFailed(Collection<String> code, String accessor, @Nullable String... args) {
        String argsStr = String.join(", ", args);

        code.add(indentedLine("if (!%s(msg.%s))", accessor, argsStr));

        indent++;

        code.add(indentedLine(RETURN_FALSE_STMT));

        indent--;
    }

    /**
     * Generate code of writing single field:
     */
    private void returnFalseIfWriteFailed(Collection<String> code, VariableElement field, String accessor, @Nullable String... args) {
        String argsStr = String.join(", ", args);

        if (type.equals(field.getEnclosingElement()))
            code.add(indentedLine("if (!%s(msg.%s))", accessor, argsStr));
        else {
            // Field has to be requested from a super class object.
            code.add(indentedLine("if (!%s(((%s)msg).%s))", accessor, field.getEnclosingElement().getSimpleName(), argsStr));
        }

        indent++;

        code.add(indentedLine(RETURN_FALSE_STMT));

        indent--;
    }

    /**
     * Generate code of writing single enum field mapped with EnumMapper:
     */
    private void returnFalseIfEnumWriteFailed(
        Collection<String> code,
        VariableElement field,
        String writerCall,
        String mapperCall,
        String fieldGetterCall) {
        if (type.equals(field.getEnclosingElement()))
            code.add(indentedLine("if (!%s(%s(msg.%s)))", writerCall, mapperCall, fieldGetterCall));
        else {
            // Field has to be requested from a super class object.
            code.add(indentedLine("if (!%s(%s(((%s)msg).%s)))",
                writerCall, mapperCall, field.getEnclosingElement().getSimpleName(), fieldGetterCall));
        }

        indent++;

        code.add(indentedLine(RETURN_FALSE_STMT));

        indent--;
    }

    /**
     * Discover access read methods, like {@code readInt}.
     *
     * @param field Field.
     */
    private void returnFalseIfReadFailed(VariableElement field) throws Exception {
        TypeMirror type = field.asType();

        boolean compress = field.getAnnotation(Compress.class) != null;

        if (compress)
            checkTypeForCompress(type);

        if (type.getKind().isPrimitive()) {
            String typeName = capitalizeOnlyFirst(type.getKind().name());

            returnFalseIfReadFailed(field, "reader.read" + typeName);

            return;
        }

        if (type.getKind() == TypeKind.ARRAY) {
            ArrayType arrType = (ArrayType)type;
            TypeMirror componentType = arrType.getComponentType();

            if (componentType.getKind().isPrimitive()) {
                String typeName = capitalizeOnlyFirst(componentType.getKind().name());

                returnFalseIfReadFailed(field, "reader.read" + typeName + "Array");

                return;
            }

            if (componentType.getKind() == TypeKind.ARRAY) {
                returnFalseIfReadFailed(field, "reader.readObjectArray", messageCollectionItemTypes(field, type));

                return;
            }

            if (componentType.getKind() == TypeKind.DECLARED) {
                Element componentElement = ((DeclaredType)componentType).asElement();

                returnFalseIfReadFailed(field, "reader.readObjectArray", messageCollectionItemTypes(field, type));

                if (!"java.lang".equals(env.getElementUtils().getPackageOf(componentElement).getQualifiedName().toString())) {
                    String importCls = ((QualifiedNameable)componentElement).getQualifiedName().toString();

                    imports.add(importCls);
                }

                return;
            }
        }

        if (type.getKind() == TypeKind.DECLARED) {
            if (sameType(type, String.class))
                returnFalseIfReadFailed(field, "reader.readString");

            else if (sameType(type, BitSet.class))
                returnFalseIfReadFailed(field, "reader.readBitSet");

            else if (sameType(type, UUID.class))
                returnFalseIfReadFailed(field, "reader.readUuid");

            else if (sameType(type, "org.apache.ignite.lang.IgniteUuid"))
                returnFalseIfReadFailed(field, "reader.readIgniteUuid");

            else if (sameType(type, "org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion"))
                returnFalseIfReadFailed(field, "reader.readAffinityTopologyVersion");

            else if (assignableFrom(erasedType(type), type(Map.class.getName()))) {
                List<? extends TypeMirror> typeArgs = ((DeclaredType)type).getTypeArguments();

                assert typeArgs.size() == 2;

                List<String> args = new ArrayList<>();

                args.add(messageCollectionItemTypes(field, type));

                if (compress)
                    args.add("true"); // the value of the compress argument in the MessageReader#readMap method

                returnFalseIfReadFailed(field, "reader.readMap", args.toArray(String[]::new));
            }

            else if (assignableFrom(type, type("org.apache.ignite.internal.processors.cache.KeyCacheObject")))
                returnFalseIfReadFailed(field, "reader.readKeyCacheObject");

            else if (assignableFrom(type, type("org.apache.ignite.internal.processors.cache.CacheObject")))
                returnFalseIfReadFailed(field, "reader.readCacheObject");

            else if (assignableFrom(type, type("org.apache.ignite.internal.util.GridLongList")))
                returnFalseIfReadFailed(field, "reader.readGridLongList");

            else if (assignableFrom(type, type(MESSAGE_INTERFACE))) {
                if (sameType(type, COMPRESSED_MESSAGE_INTERFACE))
                    throw new IllegalArgumentException(COMPRESSED_MSG_ERROR);

                if (compress)
                    returnFalseIfReadFailed(field, "reader.readMessage", "true");
                else
                    returnFalseIfReadFailed(field, "reader.readMessage");
            }

            else if (assignableFrom(erasedType(type), type(Collection.class.getName()))) {
                returnFalseIfReadFailed(field, "reader.readCollection", messageCollectionItemTypes(field, type));
            }
            else if (enumType(env, type)) {
                String fieldPrefix = typeNameToFieldName(env.getTypeUtils().asElement(type).getSimpleName().toString());

                boolean hasCustMapperAnn = field.getAnnotation(CustomMapper.class) != null;

                String mapperCallStmnt = hasCustMapperAnn ? fieldPrefix + "Mapper.decode" : "DefaultEnumMapper.INSTANCE.decode";
                String enumValsFieldName = hasCustMapperAnn ? null : fieldPrefix + "Vals";

                returnFalseIfEnumReadFailed(field, mapperCallStmnt, enumValsFieldName);
            }

            else
                throw new IllegalArgumentException("Unsupported declared type: " + type);

            return;
        }

        throw new IllegalArgumentException("Unsupported type kind: " + type.getKind());
    }

    /** */
    private String messageCollectionItemTypes(VariableElement field, TypeMirror type) throws Exception {
        String desc = messageCollectionItemTypeDescriptor(type);
        String descName = field.getSimpleName() + "CollDesc";
        String typeName = desc.substring(desc.indexOf(' ') + 1, desc.indexOf('('));

        fields.add("private static final " + typeName + " " + descName + " = " + desc + ";");

        return descName;
    }

    /** */
    private String messageCollectionItemTypeDescriptor(TypeMirror type) throws Exception {
        imports.add("org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType");

        if (type.getKind() == TypeKind.ARRAY) {
            ArrayType arrType = (ArrayType)type;
            TypeMirror componentType = arrType.getComponentType();

            String clazz;

            if (componentType.getKind() == TypeKind.ARRAY) {
                TypeMirror ctype = ((ArrayType)componentType).getComponentType();

                clazz = ctype.getKind().name().toLowerCase() + "[].class";
            }
            else if (componentType.getKind() == TypeKind.DECLARED) {
                Element componentElement = ((DeclaredType)componentType).asElement();

                clazz = componentElement.getSimpleName() + ".class";
            }
            else {
                assert componentType.getKind().isPrimitive();

                imports.add("org.apache.ignite.plugin.extensions.communication.MessageItemType");

                return "new MessageItemType(MessageCollectionItemType." + messageCollectionItemType(componentType) + "_ARR)";
            }

            imports.add("org.apache.ignite.plugin.extensions.communication.MessageArrayType");

            return "new MessageArrayType(" + messageCollectionItemTypeDescriptor(componentType) + ", " + clazz + ")";
        }
        else if (assignableFrom(erasedType(type), type(Map.class.getName()))) {
            imports.add("org.apache.ignite.plugin.extensions.communication.MessageMapType");

            List<? extends TypeMirror> typeArgs = ((DeclaredType)type).getTypeArguments();

            assert typeArgs.size() == 2;

            return "new MessageMapType(" +
                messageCollectionItemTypeDescriptor(typeArgs.get(0)) + ", " +
                messageCollectionItemTypeDescriptor(typeArgs.get(1)) + ", " +
                assignableFrom(erasedType(type), type(LinkedHashMap.class.getName())) + ")";
        }
        else if (assignableFrom(erasedType(type), type(Collection.class.getName()))) {
            imports.add("org.apache.ignite.plugin.extensions.communication.MessageCollectionType");

            List<? extends TypeMirror> typeArgs = ((DeclaredType)type).getTypeArguments();

            assert typeArgs.size() == 1;

            return "new MessageCollectionType(" +
                messageCollectionItemTypeDescriptor(typeArgs.get(0)) + ", " +
                assignableFrom(erasedType(type), type(Set.class.getName())) + ")";
        }
        else {
            imports.add("org.apache.ignite.plugin.extensions.communication.MessageItemType");

            return "new MessageItemType(MessageCollectionItemType." + messageCollectionItemType(type) + ")";
        }
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

            if (sameType(type, "org.apache.ignite.internal.processors.cache.KeyCacheObject"))
                return "KEY_CACHE_OBJECT";

            if (sameType(type, "org.apache.ignite.internal.processors.cache.CacheObject"))
                return "CACHE_OBJECT";

            if (sameType(type, "org.apache.ignite.internal.util.GridLongList"))
                return "GRID_LONG_LIST";

            PrimitiveType primitiveType = unboxedType(type);

            if (primitiveType != null)
                return primitiveType.getKind().toString();

            if (sameType(type, COMPRESSED_MESSAGE_INTERFACE))
                throw new IllegalArgumentException(COMPRESSED_MSG_ERROR);
        }

        if (!assignableFrom(type, type(MESSAGE_INTERFACE)))
            throw new Exception("Do not support type: " + type);

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
     * Generate code of reading single field.
     *
     * @param field Field.
     * @param mtd Method name.
     */
    private void returnFalseIfReadFailed(VariableElement field, String mtd, String... args) {
        String argsStr = String.join(", ", args);

        if (type.equals(field.getEnclosingElement()))
            read.add(indentedLine("msg.%s = %s(%s);", field.getSimpleName().toString(), mtd, argsStr));
        else {
            // Field has to be requested from a super class object.
            read.add(indentedLine("((%s)msg).%s = %s(%s);",
                field.getEnclosingElement().getSimpleName(), field.getSimpleName().toString(), mtd, argsStr));
        }

        read.add(EMPTY);

        read.add(indentedLine("if (!reader.isLastRead())"));

        indent++;

        read.add(indentedLine(RETURN_FALSE_STMT));

        indent--;
    }

    /**
     * Generate code of reading single field:
     *
     * @param mapperDecodeCallStmnt Method name.
     */
    private void returnFalseIfEnumReadFailed(VariableElement field, String mapperDecodeCallStmnt, String enumValuesFieldName) {
        String readOp;

        if (enumValuesFieldName == null)
            readOp = line("%s(reader.readByte())", mapperDecodeCallStmnt);
        else
            readOp = line("%s(%s, reader.readByte())", mapperDecodeCallStmnt, enumValuesFieldName);

        if (type.equals(field.getEnclosingElement()))
            read.add(indentedLine("msg.%s = %s;", field.getSimpleName().toString(), readOp));
        else {
            // Field has to be requested from a super class object.
            read.add(indentedLine("((%s)msg).%s = %s;",
                field.getEnclosingElement().getSimpleName(), field.getSimpleName().toString(), readOp));
        }

        read.add(EMPTY);

        read.add(indentedLine("if (!reader.isLastRead())"));

        indent++;

        read.add(indentedLine(RETURN_FALSE_STMT));

        indent--;
    }

    /** */
    private void finish(List<String> code, boolean read, boolean marshallable) {
        String lastLine = code.get(code.size() - 1);

        if (EMPTY.equals(lastLine))
            code.remove(code.size() - 1);

        code.add(indentedLine("}"));
        code.add(EMPTY);

        if (read && marshallable) {
            imports.add("org.apache.ignite.IgniteCheckedException");
            imports.add("org.apache.ignite.IgniteException");

            code.add(indentedLine("try {"));

            indent++;

            code.add(indentedLine("msg.finishUnmarshal(marshaller, clsLdr);"));

            indent--;

            code.add(indentedLine("}"));
            code.add(indentedLine("catch (IgniteCheckedException e) {"));

            indent++;

            code.add(indentedLine("throw new IgniteException(\"Failed to unmarshal object \" + msg.getClass().getSimpleName(), e);"));

            indent--;

            code.add(indentedLine("}"));

            code.add(EMPTY);
        }

        code.add(indentedLine("return true;"));
    }

    /**
     * Creates line with current indent from given arguments.
     *
     * @return Line with current indent.
     */
    private String indentedLine(String format, Object... args) {
        SB sb = new SB();

        for (int i = 0; i < indent; i++)
            sb.a(TAB);

        sb.a(String.format(format, args));

        return sb.toString();
    }

    /**
     * Creates line from given arguments.
     *
     * @return Line.
     */
    private String line(String format, Object... args) {
        SB sb = new SB();

        sb.a(String.format(format, args));

        return sb.toString();
    }

    /** Write serializer class fields: enum values, custom enum mappers. */
    private void writeClassFields(Writer writer) throws IOException {
        if (fields.isEmpty())
            return;

        indent = 1;

        for (String field: fields) {
            writer.write(indentedLine(METHOD_JAVADOC));
            writer.write(NL);
            writer.write(indentedLine(field));
            writer.write(NL);
        }
        writer.write(NL);

        indent = 0;
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

        imports.add(type.toString());

        if (marshallableMessage())
            imports.add("org.apache.ignite.marshaller.Marshaller");

        imports.add("org.apache.ignite.plugin.extensions.communication.MessageSerializer");
        imports.add("org.apache.ignite.plugin.extensions.communication.MessageWriter");
        imports.add("org.apache.ignite.plugin.extensions.communication.MessageReader");

        for (String regularImport : imports)
            writer.write("import " + regularImport + ";" + NL);

        writer.write(NL);
        writer.write(CLS_JAVADOC);
        writer.write(NL);

        writer.write("public class " + serClsName + " implements MessageSerializer<" + simpleNameWithGeneric(type) + "> {" + NL);
    }

    /** */
    private boolean marshallableMessage() {
        return env.getTypeUtils().isAssignable(type.asType(), marshallableMsgType);
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
    public static boolean enumType(ProcessingEnvironment env, TypeMirror type) {
        Element element = env.getTypeUtils().asElement(type);

        return element != null && element.getKind() == ElementKind.ENUM;
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
    public static boolean identicalFileIsAlreadyGenerated(ProcessingEnvironment env, String srcCode, String fqnClsName) {
        try {
            String fileName = fqnClsName.replace('.', '/') + ".java";
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
    private static String content(Reader reader) throws IOException {
        BufferedReader br = new BufferedReader(reader);
        StringBuilder sb = new StringBuilder();
        String line;

        while ((line = br.readLine()) != null)
            sb.append(line).append(NL);

        // Delete last line separator.
        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
    }

    /** Checks that the Compress annotation is used only for supported types: Map and Message. */
    private void checkTypeForCompress(TypeMirror type) {
        if (type.getKind() == TypeKind.DECLARED) {
            if (assignableFrom(erasedType(type), type(Map.class.getName())) ||
                assignableFrom(type, type(MESSAGE_INTERFACE)))
                return;
        }

        throw new IllegalArgumentException("Compress annotation is used for an unsupported type: " + type);
    }

    /** @return Simple class name. */
    private String simpleNameWithGeneric(TypeElement te) {
        if (F.size(te.getTypeParameters()) == 0)
            return te.getSimpleName().toString();

        StringBuilder generic = new StringBuilder(te.getSimpleName() + "<");

        for (int i = 0; i < F.size(te.getTypeParameters()); i++) {
            if (i > 0)
                generic.append(", ");

            generic.append("?");
        }

        generic.append(">");

        return generic.toString();
    }
}
