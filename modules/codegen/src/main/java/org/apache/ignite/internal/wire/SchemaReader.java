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

package org.apache.ignite.internal.wire;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;
import javax.lang.model.type.WildcardType;
import javax.lang.model.util.ElementFilter;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.systemview.SystemViewRowAttributeWalkerProcessor;

/** Reads class schemas from the classes being compiled. */
public class SchemaReader {
    /** Package of the serialization annotations. */
    private static final String ANNOTATIONS_PKG = Order.class.getPackageName();

    /** */
    private final ProcessingEnvironment env;

    /** */
    public SchemaReader(ProcessingEnvironment env) {
        this.env = env;
    }

    /**
     * @param type Class.
     * @param fields Fields annotated with {@link Order} in the order they are written.
     * @return Schema of the class.
     */
    public Schema read(TypeElement type, List<VariableElement> fields) {
        List<FieldRepresentation> res = new ArrayList<>();

        for (VariableElement field : fields)
            res.add(new FieldRepresentation(res.size(), typeName(field.asType()), field.getSimpleName().toString(), annotations(field)));

        List<String> clsAnnotations = new ArrayList<>();
        List<FieldRepresentation> unorderedFields = new ArrayList<>();

        for (TypeElement cls : SystemViewRowAttributeWalkerProcessor.superclasses(env, type).toList()) {
            clsAnnotations.addAll(annotations(cls));

            for (VariableElement field : ElementFilter.fieldsIn(cls.getEnclosedElements())) {
                List<String> annotations = annotations(field);

                if (field.getAnnotation(Order.class) == null && !annotations.isEmpty()) {
                    unorderedFields.add(
                        new FieldRepresentation(null, typeName(field.asType()), field.getSimpleName().toString(), annotations));
                }
            }
        }

        unorderedFields.sort(Comparator.comparing(FieldRepresentation::type).thenComparing(FieldRepresentation::name));

        res.addAll(unorderedFields);

        return new Schema(env.getElementUtils().getBinaryName(type).toString(), clsAnnotations.stream().distinct().sorted().toList(), res);
    }

    /**
     * @param fields Fields of a class.
     * @return Schemas of the enums the fields refer to.
     */
    public List<Schema> enums(List<VariableElement> fields) {
        Set<TypeElement> enums = new HashSet<>();

        for (VariableElement field : fields)
            enums.addAll(enumTypes(field.asType()));

        List<Schema> res = new ArrayList<>();

        for (TypeElement enumEl : enums) {
            String enumName = env.getElementUtils().getBinaryName(enumEl).toString();

            List<FieldRepresentation> constants = new ArrayList<>();

            for (Element el : enumEl.getEnclosedElements()) {
                if (el.getKind() == ElementKind.ENUM_CONSTANT)
                    constants.add(new FieldRepresentation(constants.size(), enumName, el.getSimpleName().toString(), List.of()));
            }

            res.add(new Schema(enumName, List.of(), constants));
        }

        return res;
    }

    /** @return Serialization annotations of an element as the compiler prints them. */
    private List<String> annotations(Element el) {
        List<String> res = new ArrayList<>();

        for (AnnotationMirror ann : el.getAnnotationMirrors()) {
            TypeElement annType = (TypeElement)ann.getAnnotationType().asElement();

            if (!env.getElementUtils().getPackageOf(annType).getQualifiedName().contentEquals(ANNOTATIONS_PKG))
                continue;

            if (!annType.getQualifiedName().contentEquals(Order.class.getName()) || ann.getElementValues().size() > 1)
                res.add(ann.toString());
        }

        Collections.sort(res);

        return res;
    }

    /** @return Enums {@code type} refers to. */
    private Set<TypeElement> enumTypes(TypeMirror type) {
        if (type.getKind() == TypeKind.ARRAY)
            return enumTypes(((ArrayType)type).getComponentType());

        Set<TypeElement> res = new HashSet<>();

        if (type.getKind() == TypeKind.DECLARED) {
            DeclaredType declared = (DeclaredType)type;

            if (declared.asElement().getKind() == ElementKind.ENUM)
                res.add((TypeElement)declared.asElement());

            for (TypeMirror arg : declared.getTypeArguments())
                res.addAll(enumTypes(arg));
        }

        return res;
    }

    /** @return Type name built by walking the type: {@link TypeMirror#toString()} differs between JDK versions. */
    private String typeName(TypeMirror type) {
        switch (type.getKind()) {
            case ARRAY:
                return typeName(((ArrayType)type).getComponentType()) + "[]";

            case DECLARED:
                DeclaredType declared = (DeclaredType)type;

                String args = declared.getTypeArguments().stream().map(this::typeName).collect(Collectors.joining(","));

                return env.getElementUtils().getBinaryName((TypeElement)declared.asElement()) + (args.isEmpty() ? "" : '<' + args + '>');

            case WILDCARD:
                WildcardType wildcard = (WildcardType)type;

                if (wildcard.getExtendsBound() != null)
                    return "? extends " + typeName(wildcard.getExtendsBound());

                return wildcard.getSuperBound() == null ? "?" : "? super " + typeName(wildcard.getSuperBound());

            case TYPEVAR:
                return ((TypeVariable)type).asElement().getSimpleName() + " extends " + typeName(env.getTypeUtils().erasure(type));

            default:
                return type.getKind().isPrimitive() ? type.getKind().name().toLowerCase(Locale.ROOT) : type.toString();
        }
    }
}
