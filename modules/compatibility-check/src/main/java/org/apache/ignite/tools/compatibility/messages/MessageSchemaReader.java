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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.util.ElementFilter;
import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import com.sun.source.util.JavacTask;
import org.apache.ignite.internal.Compress;
import org.apache.ignite.internal.CustomMapper;
import org.apache.ignite.internal.JdkMarshalled;
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.NioField;
import org.apache.ignite.internal.Order;
import org.apache.ignite.tools.compatibility.messages.dto.AnnotationRepresentation;
import org.apache.ignite.tools.compatibility.messages.dto.FieldRepresentation;
import org.apache.ignite.tools.compatibility.messages.dto.Schema;

/** Reads CLASS-retained field annotations from compiled classes using the public JDK compiler API. */
class MessageSchemaReader implements AutoCloseable {
    /** Classpath reader, closed after exporting the table. */
    private final StandardJavaFileManager files;

    /** Compiler model of existing class files; no sources or processors are executed. */
    private final JavacTask task;

    /** Creates a reader for the same classpath as the registration providers. */
    MessageSchemaReader() {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

        if (compiler == null)
            throw new IllegalStateException("A full JDK is required to read message fields");

        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));

        List<String> options = List.of("-proc:none", "-classpath", classpath);

        files = compiler.getStandardFileManager(null, Locale.ROOT, null);

        task = (JavacTask)compiler.getTask(null, files, null, options, null, List.of());
    }

    /** Returns a canonical field description, including inherited fields and marshalling annotations. */
    Schema read(Class<?> cls) {
        TypeElement msgType = task.getElements().getTypeElement(cls.getCanonicalName());

        if (msgType == null)
            throw new IllegalStateException("Message class is missing from compiler classpath: " + cls.getName());

        Deque<TypeElement> hierarchy = new ArrayDeque<>();

        TypeElement curMsgType = msgType;

        while (curMsgType.getSuperclass().getKind() != TypeKind.NONE) {
            hierarchy.addFirst(curMsgType);

            curMsgType = (TypeElement)task.getTypes().asElement(curMsgType.getSuperclass());
        }

        List<FieldRepresentation> schema = new ArrayList<>();

        List<AnnotationRepresentation> clsAnnotations = new ArrayList<>();

        if (hierarchy.stream().anyMatch(t -> t.getAnnotation(JdkMarshalled.class) != null))
            clsAnnotations.add(new AnnotationRepresentation(JdkMarshalled.class.getName(), null));

        List<FieldRepresentation> marshalledFields = new ArrayList<>();

        for (TypeElement t : hierarchy) {
            List<VariableElement> fields = new ArrayList<>(ElementFilter.fieldsIn(t.getEnclosedElements()));

            for (VariableElement field : fields) {
                Marshalled ann = field.getAnnotation(Marshalled.class);

                if (ann != null) {
                    marshalledFields.add(new FieldRepresentation(field.asType().toString(), field.getSimpleName().toString(),
                        List.of(new AnnotationRepresentation(Marshalled.class.getName(), !ann.value().isEmpty() ? "value=" + ann.value()
                        : "keys=" + ann.keys() + " values=" + ann.values()))));
                }
            }

            fields.removeIf(f -> f.getAnnotation(Order.class) == null);
            fields.sort(Comparator.comparingInt(f -> f.getAnnotation(Order.class).value()));

            for (VariableElement field : fields) {
                List<AnnotationRepresentation> annotations = new ArrayList<>();

                if (field.getAnnotation(Compress.class) != null)
                    annotations.add(new AnnotationRepresentation(Compress.class.getName(), null));

                if (field.getAnnotation(NioField.class) != null)
                    annotations.add(new AnnotationRepresentation(NioField.class.getName(), null));

                CustomMapper mapper = field.getAnnotation(CustomMapper.class);

                if (mapper != null)
                    annotations.add(new AnnotationRepresentation(CustomMapper.class.getName(), mapper.value()));

                schema.add(new FieldRepresentation(field.asType().toString(), field.getSimpleName().toString(), annotations));
            }
        }

        marshalledFields.sort(Comparator.comparing(FieldRepresentation::type)
            .thenComparing(FieldRepresentation::name)
            .thenComparing(field -> field.annotations().get(0).value()));

        schema.addAll(marshalledFields);

        return new Schema(clsAnnotations, schema);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        files.close();
    }
}
