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

package org.apache.ignite.tools.compatibility.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;
import javax.lang.model.type.WildcardType;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import com.sun.source.util.JavacTask;
import org.apache.ignite.internal.Compress;
import org.apache.ignite.internal.CustomMapper;
import org.apache.ignite.internal.JdkMarshalled;
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.NioField;
import org.apache.ignite.internal.Order;

/** Reads CLASS-retained field annotations from compiled classes using the public JDK compiler API. */
final class MessageSchema implements AutoCloseable {
    /** Classpath reader, closed after exporting the table. */
    private final StandardJavaFileManager files;

    /** Compiler model of existing class files; no sources or processors are executed. */
    private final JavacTask task;

    /** Creates a reader for the same classpath as the registration providers. */
    MessageSchema() {
        var compiler = ToolProvider.getSystemJavaCompiler();

        if (compiler == null)
            throw new IllegalStateException("A full JDK is required to read message fields");

        files = compiler.getStandardFileManager(null, Locale.ROOT, null);
        task = (JavacTask)compiler.getTask(null, files, diagnostic -> {
            if (diagnostic.getKind() == Diagnostic.Kind.ERROR)
                throw new IllegalStateException(diagnostic.toString());
        }, List.of("-proc:none", "-classpath",
            System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"))), null, List.of());
    }

    /** Returns a canonical field description, including inherited fields and marshalling annotations. */
    List<String> read(Class<?> cls) {
        TypeElement type = task.getElements().getTypeElement(cls.getCanonicalName());

        if (type == null)
            throw new IllegalStateException("Message class is missing from compiler classpath: " + cls.getName());

        List<TypeElement> hierarchy = new ArrayList<>();

        for (TypeElement t = type; t.getSuperclass().getKind() != TypeKind.NONE;
            t = (TypeElement)task.getTypes().asElement(t.getSuperclass()))
            hierarchy.add(t);

        Collections.reverse(hierarchy);

        List<String> schema = new ArrayList<>();

        if (hierarchy.stream().anyMatch(t -> t.getAnnotation(JdkMarshalled.class) != null))
            schema.add("jdkMarshalled");

        List<String> marshalledFields = new ArrayList<>();

        for (TypeElement t : hierarchy) {
            List<VariableElement> fields = new ArrayList<>(ElementFilter.fieldsIn(t.getEnclosedElements()));

            for (VariableElement field : fields) {
                Marshalled ann = field.getAnnotation(Marshalled.class);

                if (ann != null) {
                    marshalledFields.add("marshalled " + schemaType(field.asType()) + " " + field.getSimpleName()
                        + (!ann.value().isEmpty() ? " value=" + ann.value()
                        : " keys=" + ann.keys() + " values=" + ann.values()));
                }
            }

            fields.removeIf(f -> f.getAnnotation(Order.class) == null);
            fields.sort(Comparator.comparingInt(f -> f.getAnnotation(Order.class).value()));

            for (VariableElement field : fields) {
                String desc = schemaType(field.asType()) + " " + field.getSimpleName();

                if (field.getAnnotation(Compress.class) != null)
                    desc += " compress";

                if (field.getAnnotation(NioField.class) != null)
                    desc += " nio";

                CustomMapper mapper = field.getAnnotation(CustomMapper.class);

                if (mapper != null)
                    desc += " customMapper=" + mapper.value();

                schema.add(desc);
            }
        }

        Collections.sort(marshalledFields);
        schema.addAll(marshalledFields);

        return schema;
    }

    /** Returns a fully qualified type name without source-level type annotations. */
    private static String schemaType(TypeMirror type) {
        switch (type.getKind()) {
            case ARRAY:
                return schemaType(((ArrayType)type).getComponentType()) + "[]";

            case DECLARED:
            case ERROR:
                DeclaredType declaredType = (DeclaredType)type;
                String name = ((TypeElement)declaredType.asElement()).getQualifiedName().toString();
                List<? extends TypeMirror> typeArgs = declaredType.getTypeArguments();

                return name + (typeArgs.isEmpty() ? "" : typeArgs.stream()
                    .map(MessageSchema::schemaType).collect(Collectors.joining(",", "<", ">")));

            case TYPEVAR:
                return ((TypeVariable)type).asElement().getSimpleName().toString();

            case WILDCARD:
                WildcardType wildcard = (WildcardType)type;

                if (wildcard.getExtendsBound() != null)
                    return "? extends " + schemaType(wildcard.getExtendsBound());

                if (wildcard.getSuperBound() != null)
                    return "? super " + schemaType(wildcard.getSuperBound());

                return "?";

            default:
                return type.getKind().isPrimitive()
                    ? type.getKind().name().toLowerCase(Locale.ROOT)
                    : type.toString();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        files.close();
    }
}
