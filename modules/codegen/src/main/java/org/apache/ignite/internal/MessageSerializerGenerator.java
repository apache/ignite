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

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.QualifiedNameable;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import org.apache.ignite.internal.systemview.SystemViewRowAttributeWalkerProcessor;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.MessageProcessor.COMPRESSED_MESSAGE_CLASS;
import static org.apache.ignite.internal.MessageProcessor.MESSAGE_INTERFACE;

/** Generates {@code *Serializer} classes for {@link org.apache.ignite.plugin.extensions.communication.Message} types. */
public class MessageSerializerGenerator extends MessageGenerator {
    /** */
    private static final String RETURN_FALSE_STMT = "return false;";

    /** */
    static final String DLFT_ENUM_MAPPER_CLS = "org.apache.ignite.plugin.extensions.communication.mappers.DefaultEnumMapper";

    /** */
    private static final String COMPRESSED_MSG_ERROR = "CompressedMessage should not be used explicitly. " +
        "To compress the required field use the @Compress annotation.";

    /** */
    private final List<String> write = new ArrayList<>();

    /** */
    private final List<String> read = new ArrayList<>();

    /** Enum-mapper field declarations emitted at the top of the generated {@code *Serializer} class. */
    private final Set<String> fields = new java.util.TreeSet<>();

    /** */
    MessageSerializerGenerator(ProcessingEnvironment env) {
        super(env);
    }

    /** {@inheritDoc} */
    @Override String typeSuffix() {
        return "Serializer";
    }

    /** {@inheritDoc} */
    @Override void generateBody(List<VariableElement> fields) throws Exception {
        generateMethods(fields);

        // Include superclass types in imports so generated code can cast to them for inherited fields.
        SystemViewRowAttributeWalkerProcessor.superclasses(env, type).forEach(el -> imports.add(el.toString()));
    }

    /** {@inheritDoc} */
    @Override String buildClassCode(String serClsName) throws IOException {
        try (Writer writer = new StringWriter()) {
            imports.add(type.toString());
            imports.add("org.apache.ignite.plugin.extensions.communication.MessageSerializer");
            imports.add("org.apache.ignite.plugin.extensions.communication.MessageWriter");
            imports.add("org.apache.ignite.plugin.extensions.communication.MessageReader");

            writeClassHeader(writer, "MessageSerializer", serClsName);
            
            writer.write(" {" + NL);

            writeClassFields(writer);

            writeConstructor(writer, serClsName);

            for (String w: write)
                writer.write(w + NL);

            writer.write(TAB + "}" + NL + NL);

            for (String r: read)
                writer.write(r + NL);

            writer.write(TAB + "}" + NL);

            writer.write("}");

            return writer.toString();
        }
    }

    /** */
    private void writeConstructor(Writer writer, String serClsName) throws IOException {
        ++indent;

        writer.write(indentedLine(METHOD_JAVADOC));
        writer.write(NL);
        writer.write(indentedLine("public " + serClsName + "() {"));
        writer.write(NL + NL);
        writer.write(indentedLine("}"));
        writer.write(NL);
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

        finish(write);
        finish(read);
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
            write ? "writeTo" : "readFrom", write ? "MessageWriter writer" : "MessageReader reader"));

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

    /** Appends a write-fail guard for {@code field}, emitting {@code return false;} when the write fails. */
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

            else if (assignableFrom(type, type("org.apache.ignite.lang.IgniteProductVersion")))
                returnFalseIfWriteFailed(write, field, "writer.writeIgniteProductVersion", getExpr);

            else if (assignableFrom(type, type(MESSAGE_INTERFACE))) {
                if (sameType(type, COMPRESSED_MESSAGE_CLASS))
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

    /** Appends an accessor-based write-fail guard, emitting {@code return false;} when the write fails. */
    private void returnFalseIfWriteFailed(Collection<String> code, String accessor, @Nullable String... args) {
        String argsStr = String.join(", ", args);

        code.add(indentedLine("if (!%s(msg.%s))", accessor, argsStr));

        indent++;

        code.add(indentedLine(RETURN_FALSE_STMT));

        indent--;
    }

    /** Appends a field-based write-fail guard, casting to the declaring class for inherited fields. */
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

    /** Appends an enum write-fail guard using the mapper call to convert the field value before writing. */
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

    /** Appends a read-fail guard for {@code field}, returning {@code false} when reading is incomplete. */
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

            else if (assignableFrom(type, type("org.apache.ignite.lang.IgniteProductVersion")))
                returnFalseIfReadFailed(field, "reader.readIgniteProductVersion");

            else if (assignableFrom(type, type(MESSAGE_INTERFACE))) {
                if (sameType(type, COMPRESSED_MESSAGE_CLASS))
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

            if (sameType(type, COMPRESSED_MESSAGE_CLASS))
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

    /** Appends a read-fail guard using {@code mtd} to read and assign {@code field}, casting for inherited fields. */
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

    /** Appends an enum read-fail guard using the mapper decode call to set {@code field}. */
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

    /** Closes the write/read method with a final {@code return true;} statement. */
    private void finish(List<String> code) {
        String lastLine = code.get(code.size() - 1);

        if (EMPTY.equals(lastLine))
            code.remove(code.size() - 1);

        code.add(indentedLine("}"));
        code.add(EMPTY);

        code.add(indentedLine("return true;"));
    }

    /** @return {@code format} with {@code args} applied, without indentation. */
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

    /** */
    private boolean sameType(TypeMirror type, String typeStr) {
        return env.getTypeUtils().isSameType(type, type(typeStr));
    }

    /** */
    private boolean sameType(TypeMirror type, Class<?> cls) {
        return env.getTypeUtils().isSameType(type, type(cls.getCanonicalName()));
    }

    /** */
    public static boolean enumType(ProcessingEnvironment env, TypeMirror type) {
        Element element = env.getTypeUtils().asElement(type);

        return element != null && element.getKind() == ElementKind.ENUM;
    }

    /** Converts string "BYTE" to string "Byte", with first capital latter. */
    private String capitalizeOnlyFirst(String input) {
        return input.substring(0, 1).toUpperCase() + input.substring(1).toLowerCase();
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

}
