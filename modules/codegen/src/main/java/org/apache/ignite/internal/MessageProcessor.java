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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import org.apache.ignite.internal.systemview.SystemViewRowAttributeWalkerProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.MessageSerializerGenerator.DLFT_ENUM_MAPPER_CLS;
import static org.apache.ignite.internal.MessageSerializerGenerator.enumType;

/**
 * Annotation processor that generates serialization and deserialization code for classes implementing the {@code Message} interface.
 * <p>
 * This processor scans all {@code Message} classes and generates a corresponding serializer class that contains
 * {@code writeTo} and {@code readFrom} methods.
 * <p>
 * The generated serializer follows the naming convention: {@code [MessageClassName]Serializer}.
 * <p>
 * Only fields annotated with {@link Order} are included in the serialization logic.
 * <p>
 * <strong>Usage Requirements:</strong>
 * <ul>
 *   <li>The target class must implement the {@code Message} interface.</li>
 *   <li>Each field to be serialized must be annotated with {@code @Order}.</li>
 * </ul>
 *
 * <p>
 * This processor is typically registered using the {@code META-INF/services/javax.annotation.processing.Processor}
 * service file and triggered during the compilation phase.
 */
@SupportedAnnotationTypes("org.apache.ignite.internal.Order")
@SupportedSourceVersion(SourceVersion.RELEASE_17)
public class MessageProcessor extends AbstractProcessor {
    /** Base interface that every message must implement. */
    static final String MESSAGE_INTERFACE = "org.apache.ignite.plugin.extensions.communication.Message";

    /** Compressed message. */
    static final String COMPRESSED_MESSAGE_CLASS = "org.apache.ignite.internal.managers.communication.CompressedMessage";

    /** Externalizable message. */
    static final String MARSHALLABLE_MESSAGE_INTERFACE = "org.apache.ignite.internal.MarshallableMessage";

    /** Marker of messages with no marshaller. */
    static final String NON_MARSHALLABLE_MESSAGE_INTERFACE = "org.apache.ignite.plugin.extensions.communication.NonMarshallableMessage";

    /** */
    static final String CACHE_OBJECT_CLS = "org.apache.ignite.internal.processors.cache.CacheObject";

    /** */
    static final String KEY_CACHE_OBJECT_CLS = "org.apache.ignite.internal.processors.cache.KeyCacheObject";

    /** Checked exception declared by the generated methods. */
    static final String IGNITE_CHECKED_EXCEPTION_CLS = "org.apache.ignite.IgniteCheckedException";

    /** */
    public static final String GRID_H2_NULL = "org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2Null";

    /** */
    public static final String ZK_NO_SERVERS_MESSAGE = "org.apache.ignite.spi.discovery.zk.internal.ZkNoServersMessage";

    /** */
    public static final Set<String> NO_PUBLIC_CTOR_MSGS = Set.of(GRID_H2_NULL, ZK_NO_SERVERS_MESSAGE);

    /** Messages with no fields. A serializer must be generated due to restrictions in our communication process. */
    static final String[] EMPTY_MESSAGES = {
        "org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage",
        ZK_NO_SERVERS_MESSAGE,
        GRID_H2_NULL,
    };

    /** Messages with no fields. A serializer generation intentionally skipped. */
    static final String[] SKIP_MESSAGES = {
        "org.apache.ignite.internal.processors.odbc.ClientMessage",
        COMPRESSED_MESSAGE_CLASS,
        "org.apache.ignite.loadtests.communication.GridTestMessage",
        "org.apache.ignite.spi.communication.tcp.TestDelayMessage"
    };

    /** */
    private final Map<String, IgniteBiTuple<String, String>> enumMappersInUse = new HashMap<>();

    /** Processes all classes implementing the {@code Message} interface and generates corresponding serializer code. */
    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        TypeElement msgEl = processingEnv.getElementUtils().getTypeElement(MESSAGE_INTERFACE);

        if (msgEl == null) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                "Cannot resolve " + MESSAGE_INTERFACE + " on the annotation-processing classpath.");

            return false;
        }

        TypeMirror msgType = msgEl.asType();

        List<TypeMirror> emptyMsgs = typesToTypeMirrors(EMPTY_MESSAGES);
        List<TypeMirror> skipMsgs = typesToTypeMirrors(SKIP_MESSAGES);

        TypeElement marshallableEl = processingEnv.getElementUtils().getTypeElement(MARSHALLABLE_MESSAGE_INTERFACE);
        TypeElement nonMarshallableEl = processingEnv.getElementUtils().getTypeElement(NON_MARSHALLABLE_MESSAGE_INTERFACE);

        Map<TypeElement, List<VariableElement>> msgFields = new HashMap<>();

        for (Element el: roundEnv.getRootElements()) {
            if (el.getKind() != ElementKind.CLASS)
                continue;

            TypeElement clazz = (TypeElement)el;

            if (!isAssignable(msgType, clazz))
                continue;

            // No marshaller is generated for a NonMarshallableMessage, so declared marshalling logic would silently never run.
            if (nonMarshallableEl != null && isAssignable(nonMarshallableEl.asType(), clazz)
                && ((marshallableEl != null && isAssignable(marshallableEl.asType(), clazz)) || hasMarshalledFields(clazz))) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "NonMarshallableMessage must not implement MarshallableMessage or declare @Marshalled fields", clazz);
            }

            if (clazz.getModifiers().contains(Modifier.ABSTRACT))
                continue;

            List<VariableElement> fields = orderedFields(clazz);

            if (fields.isEmpty() && emptyMsgs.stream().noneMatch(t -> isAssignable(t, clazz))) {
                if (skipMsgs.stream().anyMatch(t -> isAssignable(t, clazz)))
                    continue;

                processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "Message class doesn't have any ordered fields. " +
                        "Annotate fields with @Order or add to known empty classes MessageProcessor#EMPTY_MESSAGES",
                    clazz);
            }

            if (!checkConstructors(clazz))
                continue;

            msgFields.put(clazz, fields);
        }

        List<Function<ProcessingEnvironment, MessageCompanionGenerator>> generators = List.of(
            MessageSerializerGenerator::new, MessageMarshallerGenerator::new, MessageDeploymentGenerator::new);

        for (Map.Entry<TypeElement, List<VariableElement>> type: msgFields.entrySet()) {
            for (Function<ProcessingEnvironment, MessageCompanionGenerator> factory : generators) {
                MessageCompanionGenerator gen = factory.apply(processingEnv);

                try {
                    gen.generate(type.getKey(), type.getValue());
                }
                catch (Exception e) {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                        "Failed to generate a message " + gen.typeSuffix().toLowerCase() + ":" + e.getMessage(), type.getKey());
                }
            }
        }

        return true;
    }

    /** */
    private boolean checkConstructors(TypeElement clazz) {
        if (NO_PUBLIC_CTOR_MSGS.contains(clazz.getQualifiedName().toString()))
            return true;

        for (Element el : clazz.getEnclosedElements()) {
            if (el.getKind() != ElementKind.CONSTRUCTOR || !el.getModifiers().contains(Modifier.PUBLIC))
                continue;

            ExecutableElement c = (ExecutableElement)el;

            boolean isDfltConstructor = F.isEmpty(c.getParameters());

            if (isDfltConstructor)
                return true;
        }

        processingEnv.getMessager().printMessage(
            Diagnostic.Kind.ERROR,
            "A class must have a public default constructor: " + clazz.getQualifiedName(),
            clazz
        );

        return false;
    }

    /**
     * Collects all fields annotated with {@link Order} from the given {@link TypeElement} and all its superclasses.
     * <p>
     * The resulting list is sorted in ascending order of the {@code @Order} value.
     *
     * @param type the {@code TypeElement} representing the class to inspect.
     * @return all ordered fields including those in superclasses, sorted by ascending {@code @Order} value.
     */
    private List<VariableElement> orderedFields(TypeElement type) {
        List<List<VariableElement>> hierList = hierarchicalOrderedFields(type);

        List<VariableElement> result = new ArrayList<>();

        for (List<VariableElement> elList : hierList) {
            elList.sort(Comparator.comparingInt(f -> f.getAnnotation(Order.class).value()));

            result.addAll(elList);

            for (int i = 0; i < elList.size(); i++) {
                if (elList.get(i).getAnnotation(Order.class).value() != i) {
                    processingEnv.getMessager().printMessage(
                        Diagnostic.Kind.ERROR,
                        "Annotation @Order must be a sequence from 0 to " + (elList.size() - 1),
                        elList.get(i));
                }
            }
        }

        return result;
    }

    /** */
    private List<List<VariableElement>> hierarchicalOrderedFields(TypeElement type) {
        Element superType = processingEnv.getTypeUtils().asElement(type.getSuperclass());

        List<List<VariableElement>> hierList = superType == null ? new ArrayList<>() : hierarchicalOrderedFields((TypeElement)superType);

        List<VariableElement> elList = new ArrayList<>();

        for (Element el : type.getEnclosedElements()) {
            if (el.getAnnotation(Order.class) != null) {
                elList.add((VariableElement)el);

                if (el.getModifiers().contains(Modifier.STATIC)) {
                    processingEnv.getMessager().printMessage(
                        Diagnostic.Kind.ERROR,
                        "Annotation @Order must only be used for non-static fields.",
                        el);
                }

                validateEnumFieldMapping(type, el);
            }
        }

        hierList.add(elList);

        return hierList;
    }

    /**
     * Validates consistency of enum field mappers configuration: the same mapper is used for the same enum across different messages,
     * CustomMapper annotation is used only for enum fields.
     *
     * @param type Type implementing Message interface.
     * @param el Enclosed element of the type.
     */
    private void validateEnumFieldMapping(TypeElement type, Element el) {
        CustomMapper custMappAnn = el.getAnnotation(CustomMapper.class);

        Map<Element, String> enumsPerField = new HashMap<>();

        if (!inspectFieldForEnumTypes(type.toString(), el, el.asType(), custMappAnn, enumsPerField) && custMappAnn != null) {
            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                "Annotation @CustomMapper must only be used for enum fields or enum collections and maps, including nested ones.",
                el);
        }
    }

    /**
     * @param msgClsName Message class name currently being inspected.
     * @param field Field being inspected.
     * @param type Type that should be inpected for enum type (direct type or type parameter).
     * @param custMappAnn Custom mapper annotation declared for the enum type.
     * @param enumsPerField Map for collecting enum types related to a partiular field.
     */
    private boolean inspectFieldForEnumTypes(String msgClsName, Element field, TypeMirror type, CustomMapper custMappAnn,
        Map<Element, String> enumsPerField) {
        String enumClsFullName = type.toString();
        String enumMapperClsName = custMappAnn != null ? custMappAnn.value() : DLFT_ENUM_MAPPER_CLS;

        if (enumType(processingEnv, type)) {
            inspectForDuplicatedMappers(msgClsName, field, enumClsFullName, enumMapperClsName);

            inspectForDuplicatedEnums(msgClsName, field, type, enumsPerField);

            return true;
        }
        else if (assignableFrom(erasedType(type), type(Collection.class.getName()))) {
            List<? extends TypeMirror> typeArgs = ((DeclaredType)type).getTypeArguments();

            assert typeArgs.size() == 1 : type.toString();

            TypeMirror typeArg = typeArgs.get(0);

            return inspectFieldForEnumTypes(msgClsName, field, typeArg, custMappAnn, enumsPerField);
        }
        else if (assignableFrom(erasedType(type), type(Map.class.getName()))) {
            List<? extends TypeMirror> typeArgs = ((DeclaredType)type).getTypeArguments();

            assert typeArgs.size() == 2 : type.toString();

            TypeMirror keyType = typeArgs.get(0);
            TypeMirror valType = typeArgs.get(1);

            return inspectFieldForEnumTypes(msgClsName, field, keyType, custMappAnn, enumsPerField) |
                inspectFieldForEnumTypes(msgClsName, field, valType, custMappAnn, enumsPerField);
        }

        return false;
    }

    /**
     * Checks, that only single type of mapper is used for the enum type.
     * Particular enum should be processed with a concrete enum mapper: custom or default one.
     */
    private void inspectForDuplicatedMappers(String msgClsName, Element field, String enumClsFullName,
        String enumMapperClsName) {
        IgniteBiTuple<String, String> otherMsgAndMapperClassesNames =
            enumMappersInUse.put(enumClsFullName, new IgniteBiTuple<>(msgClsName, enumMapperClsName));

        if (otherMsgAndMapperClassesNames != null) {
            String otherMsgClsName = otherMsgAndMapperClassesNames.get1();
            String otherEnumMapperClsName = otherMsgAndMapperClassesNames.get2();

            if (!otherEnumMapperClsName.equals(enumMapperClsName)) {
                processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "Enum " + enumClsFullName + " is declared with different mappers: " +
                        otherEnumMapperClsName + " in " + otherMsgClsName + " and " +
                        enumMapperClsName + " in " + msgClsName +
                        ". Only one mapper is allowed per enum type.",
                    field);
            }
        }
    }

    /**
     * Checks that only single type of enum is introduced for a particular field.
     * Multiple enum types currently are not supported for custom mapper.
     */
    private void inspectForDuplicatedEnums(String msgClsName, Element field, TypeMirror type,
        Map<Element, String> enumsPerField) {
        String otherEnum = type.toString();
        String existingEnum = enumsPerField.put(field, otherEnum);

        if (existingEnum != null && !Objects.equals(existingEnum, otherEnum)) {
            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                String.format("Multiple enums of different types are not supported for a single field " +
                        "[msgClsName=%s, field=%s, existingEnumType=%s, otherEnumType=%s]", msgClsName,
                    field.getSimpleName(), existingEnum, otherEnum));
        }
    }

    /** Map class names to {@link TypeMirror} objects. */
    private List<TypeMirror> typesToTypeMirrors(String[] types) {
        return Arrays.stream(types)
            .map(cls -> processingEnv.getElementUtils().getTypeElement(cls))
            .filter(Objects::nonNull)
            .map(Element::asType)
            .collect(Collectors.toList());
    }

    /** */
    private boolean isAssignable(TypeMirror t, TypeElement clazz) {
        return processingEnv.getTypeUtils().isAssignable(clazz.asType(), t);
    }

    /** @return {@code true} if {@code clazz} or any of its superclasses declares a {@code @Marshalled} field. */
    private boolean hasMarshalledFields(TypeElement clazz) {
        return SystemViewRowAttributeWalkerProcessor.superclasses(processingEnv, clazz)
            .flatMap(c -> ElementFilter.fieldsIn(c.getEnclosedElements()).stream())
            .anyMatch(f -> f.getAnnotation(Marshalled.class) != null);
    }

    /** */
    boolean assignableFrom(TypeMirror type, TypeMirror superType) {
        return superType != null && processingEnv.getTypeUtils().isAssignable(type, superType);
    }

    /** */
    TypeMirror erasedType(TypeMirror type) {
        return processingEnv.getTypeUtils().erasure(type);
    }

    /** @return the {@link TypeMirror} for the fully-qualified {@code clazz}, or {@code null} if not on classpath. */
    TypeMirror type(String clazz) {
        Elements elementUtils = processingEnv.getElementUtils();
        TypeElement typeElement = elementUtils.getTypeElement(clazz);
        return typeElement != null ? typeElement.asType() : null;
    }
}
