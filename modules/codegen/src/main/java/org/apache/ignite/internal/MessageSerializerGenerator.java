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
import java.util.TreeSet;
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
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.MessageProcessor.COMPRESSED_MESSAGE_CLASS;
import static org.apache.ignite.internal.MessageProcessor.MESSAGE_INTERFACE;

/** Generates {@code *Serializer} classes for {@code Message} types. */
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

    /** Class-field declarations (enum mappers/values, collection descriptors) emitted at the top of the generated class. */
    private final Set<String> clsFields = new TreeSet<>();

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

            writeDefaultConstructor(writer, serClsName);

            for (String w: write)
                writer.write(w + NL);

            writer.write(NL);

            for (String r: read)
                writer.write(r + NL);

            writer.write("}");

            return writer.toString();
        }
    }

    /** Generates code for {@code writeTo} and {@code readFrom}. */
    private void generateMethods(List<VariableElement> fields) throws Exception {
        generateMethod(write, fields, true);
        generateMethod(read, fields, false);
    }

    /**
     * Generates the complete {@code writeTo}/{@code readFrom} method:
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
     * @param fields Ordered fields.
     * @param write Whether write code is generated.
     */
    private void generateMethod(List<String> code, List<VariableElement> fields, boolean write) throws Exception {
        code.add(indentedLine(METHOD_JAVADOC));

        code.add(indentedLine("@Override public boolean %s(" + simpleNameWithGeneric(type) + " msg, %s) {",
            write ? "writeTo" : "readFrom", write ? "MessageWriter writer" : "MessageReader reader"));

        indent++;

        if (write) {
            code.add(indentedLine("if (!writer.isHeaderWritten()) {"));

            indent++;

            returnFalseIf(code, "!writer.writeHeader(msg.directType())");

            code.add(EMPTY);
            code.add(indentedLine("writer.onHeaderWritten();"));

            indent--;

            code.add(indentedLine("}"));
            code.add(EMPTY);
        }

        code.add(indentedLine("switch (%s.state()) {", write ? "writer" : "reader"));

        indent++;

        int state = 0;

        for (VariableElement field: fields)
            processField(field, state++, write);

        indent--;

        finish(code);

        indent--;

        code.add(indentedLine("}"));
    }

    /**
     * @param field Field.
     * @param opt Case option.
     * @param write Whether write code is generated.
     */
    private void processField(VariableElement field, int opt, boolean write) throws Exception {
        if (assignableFrom(field.asType(), type(Throwable.class.getName())))
            throw new UnsupportedOperationException("You should use ErrorMessage for serialization of throwables.");

        if (write)
            writeField(opt, callExpr(field, true));
        else
            readField(field, opt, callExpr(field, false));
    }

    /** @return Writer/reader call expression for {@code field}. */
    private String callExpr(VariableElement field, boolean write) throws Exception {
        if (enumType(env, field.asType())) {
            String prefix = registerEnumMapper(field);

            boolean custMapper = field.getAnnotation(CustomMapper.class) != null;

            if (write) {
                return "writer.writeByte(" + (custMapper ? prefix + "Mapper" : "DefaultEnumMapper.INSTANCE") +
                    ".encode(" + fieldRef(field) + "))";
            }

            return custMapper
                ? prefix + "Mapper.decode(reader.readByte())"
                : "DefaultEnumMapper.INSTANCE.decode(" + prefix + "Vals, reader.readByte())";
        }

        FieldCall call = fieldCall(field);

        return write ? call.expr("writer.write", fieldRef(field)) : call.expr("reader.read", null);
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
     * @param opt Case option.
     * @param writeExpr Writer call expression.
     */
    private void writeField(int opt, String writeExpr) {
        write.add(indentedLine("case %d:", opt));

        indent++;

        returnFalseIf(write, "!" + writeExpr);

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
     * @param readExpr Reader call expression.
     */
    private void readField(VariableElement field, int opt, String readExpr) {
        read.add(indentedLine("case %d:", opt));

        indent++;

        read.add(indentedLine("%s = %s;", fieldRef(field), readExpr));
        read.add(EMPTY);

        returnFalseIf(read, "!reader.isLastRead()");

        read.add(EMPTY);
        read.add(indentedLine("reader.incrementState();"));
        read.add(EMPTY);

        indent--;
    }

    /**
     * Resolves the writer/reader call for {@code field} from its type, registering required imports and
     * collection-descriptor class fields. Enum fields are handled separately in {@link #processField}.
     */
    private FieldCall fieldCall(VariableElement field) throws Exception {
        TypeMirror type = field.asType();

        boolean compress = field.getAnnotation(Compress.class) != null;

        if (compress)
            checkTypeForCompress(type);

        if (type.getKind().isPrimitive())
            return new FieldCall(capitalizeOnlyFirst(type.getKind().name()), null, false);

        if (type.getKind() == TypeKind.ARRAY) {
            TypeMirror compType = ((ArrayType)type).getComponentType();

            if (compType.getKind().isPrimitive())
                return new FieldCall(capitalizeOnlyFirst(compType.getKind().name()) + "Array", null, false);

            if (compType.getKind() == TypeKind.DECLARED) {
                Element compElem = ((DeclaredType)compType).asElement();

                if (!"java.lang".equals(env.getElementUtils().getPackageOf(compElem).getQualifiedName().toString()))
                    imports.add(((QualifiedNameable)compElem).getQualifiedName().toString());
            }

            return new FieldCall("ObjectArray", messageCollectionItemTypes(field, type), false);
        }

        if (type.getKind() == TypeKind.DECLARED) {
            if (sameType(type, String.class))
                return new FieldCall("String", null, false);

            if (sameType(type, BitSet.class))
                return new FieldCall("BitSet", null, false);

            if (sameType(type, UUID.class))
                return new FieldCall("Uuid", null, false);

            if (sameType(type, "org.apache.ignite.lang.IgniteUuid"))
                return new FieldCall("IgniteUuid", null, false);

            if (sameType(type, "org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion"))
                return new FieldCall("AffinityTopologyVersion", null, false);

            if (assignableFrom(erasedType(type), type(Map.class.getName())))
                return new FieldCall("Map", messageCollectionItemTypes(field, type), compress);

            if (assignableFrom(type, type("org.apache.ignite.internal.processors.cache.KeyCacheObject")))
                return new FieldCall("KeyCacheObject", null, false);

            if (assignableFrom(type, type("org.apache.ignite.internal.processors.cache.CacheObject")))
                return new FieldCall("CacheObject", null, false);

            if (assignableFrom(type, type("org.apache.ignite.internal.util.GridLongList")))
                return new FieldCall("GridLongList", null, false);

            if (assignableFrom(type, type("org.apache.ignite.lang.IgniteProductVersion")))
                return new FieldCall("IgniteProductVersion", null, false);

            if (assignableFrom(type, type("org.apache.ignite.internal.processors.cache.version.GridCacheVersion")))
                return new FieldCall("GridCacheVersion", null, false);

            if (assignableFrom(type, type(MESSAGE_INTERFACE))) {
                if (sameType(type, COMPRESSED_MESSAGE_CLASS))
                    throw new IllegalArgumentException(COMPRESSED_MSG_ERROR);

                return new FieldCall("Message", null, compress);
            }

            if (assignableFrom(erasedType(type), type(Collection.class.getName())))
                return new FieldCall("Collection", messageCollectionItemTypes(field, type), false);

            throw new IllegalArgumentException("Unsupported declared type: " + type);
        }

        throw new IllegalArgumentException("Unsupported type kind: " + type.getKind());
    }

    /**
     * Registers imports and the mapper/values class field for an enum {@code field}.
     *
     * @return Class-field name prefix derived from the enum type name.
     */
    private String registerEnumMapper(VariableElement field) {
        Element enumElem = env.getTypeUtils().asElement(field.asType());

        imports.add(enumElem.toString());

        String enumName = enumElem.getSimpleName().toString();
        String prefix = typeNameToFieldName(enumName);

        CustomMapper custMapperAnn = field.getAnnotation(CustomMapper.class);

        if (custMapperAnn != null) {
            String fullMapperName = custMapperAnn.value();

            if (fullMapperName == null || fullMapperName.isEmpty())
                throw new IllegalArgumentException("Please specify a not-null not-empty EnumMapper class name");

            imports.add("org.apache.ignite.plugin.extensions.communication.mappers.EnumMapper");
            imports.add(fullMapperName);

            String simpleName = fullMapperName.substring(fullMapperName.lastIndexOf('.') + 1);

            clsFields.add("private final EnumMapper<" + enumName + "> " + prefix + "Mapper = new " + simpleName + "();");
        }
        else {
            imports.add(DLFT_ENUM_MAPPER_CLS);

            clsFields.add("private final " + enumName + "[] " + prefix + "Vals = " + enumName + ".values();");
        }

        return prefix;
    }

    /** @return {@code "msg.<field>"} accessor, cast to the declaring superclass for inherited fields. */
    private String fieldRef(VariableElement field) {
        String ref = type.equals(field.getEnclosingElement())
            ? "msg"
            : "((" + field.getEnclosingElement().getSimpleName() + ")msg)";

        return ref + "." + field.getSimpleName();
    }

    /** Appends {@code if (<cond>) return false;} to {@code code}. */
    private void returnFalseIf(Collection<String> code, String cond) {
        code.add(indentedLine("if (%s)", cond));

        indent++;

        code.add(indentedLine(RETURN_FALSE_STMT));

        indent--;
    }

    /**
     * Converts type name to camel case field name. Example: {@code "MyType"} -> {@code "myType"}.
     */
    private String typeNameToFieldName(String typeName) {
        char[] typeNameChars = typeName.toCharArray();
        typeNameChars[0] = Character.toLowerCase(typeNameChars[0]);
        return new String(typeNameChars);
    }

    /** */
    private String messageCollectionItemTypes(VariableElement field, TypeMirror type) throws Exception {
        String desc = messageCollectionItemTypeDescriptor(type);
        String descName = field.getSimpleName() + "CollDesc";
        String typeName = desc.substring(desc.indexOf(' ') + 1, desc.indexOf('('));

        clsFields.add("private static final " + typeName + " " + descName + " = " + desc + ";");

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

            if (sameType(type, "org.apache.ignite.internal.processors.cache.version.GridCacheVersion"))
                return "GRID_CACHE_VERSION";

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

    /** Closes the {@code switch} and emits the final {@code return true;} statement. */
    private void finish(List<String> code) {
        String lastLine = code.get(code.size() - 1);

        if (EMPTY.equals(lastLine))
            code.remove(code.size() - 1);

        code.add(indentedLine("}"));
        code.add(EMPTY);

        code.add(indentedLine("return true;"));
    }

    /** Write serializer class fields: enum values, custom enum mappers, collection descriptors. */
    private void writeClassFields(Writer writer) throws IOException {
        if (clsFields.isEmpty())
            return;

        for (String field: clsFields) {
            writer.write(indentedLine(METHOD_JAVADOC));
            writer.write(NL);
            writer.write(indentedLine(field));
            writer.write(NL);
        }
        writer.write(NL);
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

    /** Writer/reader call for a field: method name suffix with optional collection-descriptor and compress arguments. */
    private static final class FieldCall {
        /** Suffix appended to {@code writer.write}/{@code reader.read} to form the method name. */
        private final String mtd;

        /** Name of the generated collection-descriptor class field, or {@code null} if not needed. */
        @Nullable private final String collDesc;

        /** Whether the {@code compress = true} argument is appended. */
        private final boolean compress;

        /** */
        private FieldCall(String mtd, @Nullable String collDesc, boolean compress) {
            this.mtd = mtd;
            this.collDesc = collDesc;
            this.compress = compress;
        }

        /** @return Full call expression; {@code valArg}, when given, is passed as the first argument (write side). */
        private String expr(String mtdPrefix, @Nullable String valArg) {
            List<String> args = new ArrayList<>();

            if (valArg != null)
                args.add(valArg);

            if (collDesc != null)
                args.add(collDesc);

            if (compress)
                args.add("true");

            return mtdPrefix + mtd + "(" + String.join(", ", args) + ")";
        }
    }
}
