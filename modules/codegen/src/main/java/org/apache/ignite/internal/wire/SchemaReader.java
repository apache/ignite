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
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;
import javax.lang.model.type.WildcardType;
import javax.lang.model.util.ElementFilter;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.systemview.SystemViewRowAttributeWalkerProcessor;

/** Reads class schemas from the classes being compiled. */
public class SchemaReader {
    /** Package of the serialization annotations. An annotation of another package does not affect the wire format. */
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

        // The fields come in the order they are written, so the position of a field is its order in the whole hierarchy.
        for (VariableElement field : fields)
            res.add(new FieldRepresentation(res.size(), typeName(field.asType()), simpleName(field), annotations(field)));

        List<String> clsAnnotations = new ArrayList<>();
        List<FieldRepresentation> logicalFields = new ArrayList<>();

        for (TypeElement cls : SystemViewRowAttributeWalkerProcessor.superclasses(env, type).collect(Collectors.toList())) {
            clsAnnotations.addAll(annotations(cls));

            for (VariableElement field : ElementFilter.fieldsIn(cls.getEnclosedElements())) {
                List<String> annotations = annotations(field);

                if (field.getAnnotation(Order.class) == null && !annotations.isEmpty())
                    logicalFields.add(new FieldRepresentation(null, typeName(field.asType()), simpleName(field), annotations));
            }
        }

        logicalFields.sort(Comparator.comparing(FieldRepresentation::type).thenComparing(FieldRepresentation::name));

        res.addAll(logicalFields);

        return new Schema(binaryName(type), clsAnnotations.stream().distinct().sorted().collect(Collectors.toList()), res);
    }

    /**
     * Takes the serialization annotations of an element as the compiler prints them, the way they are written in the code,
     * with no knowledge of what is inside. {@link Order} is skipped unless it sets more than the order, which is already
     * described by the position of a field.
     */
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

    /**
     * Prints a type by walking it: {@link TypeMirror#toString()} includes type annotations and differs between JDK versions.
     *
     * @return Binary names of the classes, type arguments in angle brackets separated with a comma, a type variable
     *      with its bound.
     */
    private String typeName(TypeMirror type) {
        switch (type.getKind()) {
            case ARRAY:
                return typeName(((ArrayType)type).getComponentType()) + "[]";

            case DECLARED:
                DeclaredType declared = (DeclaredType)type;

                String args = declared.getTypeArguments().stream().map(this::typeName).collect(Collectors.joining(","));

                return binaryName((TypeElement)declared.asElement()) + (args.isEmpty() ? "" : '<' + args + '>');

            case WILDCARD:
                WildcardType wildcard = (WildcardType)type;

                if (wildcard.getExtendsBound() != null)
                    return "? extends " + typeName(wildcard.getExtendsBound());

                return wildcard.getSuperBound() == null ? "?" : "? super " + typeName(wildcard.getSuperBound());

            case TYPEVAR:
                // The bound defines how a field is written. It is erased, because it may refer to the variable itself.
                return ((TypeVariable)type).asElement().getSimpleName() + " extends " + typeName(env.getTypeUtils().erasure(type));

            default:
                return type.getKind().isPrimitive() ? type.getKind().name().toLowerCase(Locale.ROOT) : type.toString();
        }
    }

    /** */
    private String binaryName(TypeElement type) {
        return env.getElementUtils().getBinaryName(type).toString();
    }

    /** */
    private String simpleName(Element el) {
        return el.getSimpleName().toString();
    }
}
