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

package org.apache.ignite.internal.idto;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.FilerException;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

import static org.apache.ignite.internal.MessageSerializerGenerator.NL;
import static org.apache.ignite.internal.MessageSerializerGenerator.TAB;
import static org.apache.ignite.internal.MessageSerializerGenerator.identicalFileIsAlreadyGenerated;
import static org.apache.ignite.internal.idto.IDTOSerializerGenerator.CLS_JAVADOC;
import static org.apache.ignite.internal.idto.IDTOSerializerGenerator.DTO_SERDES_INTERFACE;
import static org.apache.ignite.internal.idto.IDTOSerializerGenerator.PKG_NAME;
import static org.apache.ignite.internal.idto.IDTOSerializerGenerator.simpleName;

/**
 *
 */
@SupportedAnnotationTypes("org.apache.ignite.internal.management.api.Argument")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class IgniteDataTransferObjectProcessor extends AbstractProcessor {
    /** Base interface that every message must implement. */
    private static final String DTO_CLASS = "org.apache.ignite.internal.dto.IgniteDataTransferObject";

    /** Base interface that every message must implement. */
    private static final String ARG_ANNOTATION = "org.apache.ignite.internal.management.api.Argument";

    /** Factory class name. */
    public static final String FACTORY_CLASS = "IDTOSerializerFactory";

    /**
     * Processes all classes extending the {@code IgniteDataTransferObject} and generates corresponding serializer code.
     */
    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (roundEnv.errorRaised())
            return true;

        TypeMirror dtoCls = processingEnv.getElementUtils().getTypeElement(DTO_CLASS).asType();
        TypeMirror argAnnotation = processingEnv.getElementUtils().getTypeElement(ARG_ANNOTATION).asType();

        Map<TypeElement, String> genSerDes = new HashMap<>();

        for (Element el: roundEnv.getRootElements()) {
            if (el.getKind() != ElementKind.CLASS)
                continue;

            TypeElement clazz = (TypeElement)el;

            if (!processingEnv.getTypeUtils().isAssignable(clazz.asType(), dtoCls))
                continue;

            if (clazz.getModifiers().contains(Modifier.ABSTRACT))
                continue;

            if (!clazz.getModifiers().contains(Modifier.PUBLIC))
                continue;

            if (!hasArgumentFields(clazz, argAnnotation))
                continue;

            try {
                IDTOSerializerGenerator gen = new IDTOSerializerGenerator(processingEnv, clazz);

                if (gen.generate() && !clazz.toString().contains("SystemView"))
                    genSerDes.put(clazz, gen.serializerFQN());
            }
            catch (Exception e) {
                processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "Failed to generate a dto serializer:" + e.getMessage(),
                    clazz
                );
            }
        }

        if (genSerDes.isEmpty())
            return true;

        try {
            String factoryFQN = PKG_NAME + "." + FACTORY_CLASS;
            String factoryCode = generateFactory(genSerDes);

            try {
                JavaFileObject file = processingEnv.getFiler().createSourceFile(factoryFQN);

                try (Writer writer = file.openWriter()) {
                    writer.append(factoryCode);
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
                if (!identicalFileIsAlreadyGenerated(processingEnv, factoryCode, PKG_NAME, FACTORY_CLASS)) {
                    processingEnv.getMessager().printMessage(
                        Diagnostic.Kind.ERROR,
                        FACTORY_CLASS + " is already generated. Try 'mvn clean install' to fix the issue.");

                    throw e;
                }
            }
        }
        catch (Exception e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "Failed to generate a dto factory:" + e.getMessage());
        }

        return true;
    }

    /** */
    private String generateFactory(Map<TypeElement, String> genSerDes) throws IOException {
        try (Writer writer = new StringWriter()) {
            writeClassHeader(writer, genSerDes.keySet());

            writer.write(TAB);
            writer.write("/** */");
            writer.write(NL);
            writer.write(TAB);
            writer.write("private static final " + FACTORY_CLASS + " instance = new " + FACTORY_CLASS + "();");
            writer.write(NL);
            writer.write(NL);

            writer.write(TAB);
            writer.write("/** */");
            writer.write(NL);
            writer.write(TAB);
            writer.write("private final Map<Class<?>, " + simpleName(DTO_SERDES_INTERFACE) + "> serdes = new HashMap<>();");
            writer.write(NL);
            writer.write(NL);

            constructor(genSerDes, writer);
            writer.write(NL);

            getInstance(writer);
            writer.write(NL);

            serializer(writer);

            writer.write("}");
            writer.write(NL);

            return writer.toString();
        }
    }

    /** */
    private void serializer(Writer writer) throws IOException {
        writer.write(TAB);
        writer.write("/** */");
        writer.write(NL);
        writer.write(TAB);

        String genericType = "<T extends " + simpleName(DTO_CLASS) + ">";

        writer.write("public " + genericType + " " + simpleName(DTO_SERDES_INTERFACE) + "<T> serializer(Class<T> cls) {");
        writer.write(NL);
        writer.write(TAB);
        writer.write(TAB);
        writer.write("IgniteDataTransferObjectSerializer<T> res = (IgniteDataTransferObjectSerializer<T>)serdes.get(cls);");
        writer.write(NL);
        writer.write(NL);
        writer.write(TAB);
        writer.write(TAB);
        writer.write("if (res == null && getClass().desiredAssertionStatus()) {");
        writer.write(NL);
        writer.write(TAB);
        writer.write(TAB);
        writer.write(TAB);
        writer.write("res = U.loadSerializer(cls);");
        writer.write(NL);
        writer.write(NL);
        writer.write(TAB);
        writer.write(TAB);
        writer.write(TAB);
        writer.write("serdes.put(cls, res);");
        writer.write(NL);
        writer.write(TAB);
        writer.write(TAB);
        writer.write("}");
        writer.write(NL);
        writer.write(NL);
        writer.write(TAB);
        writer.write(TAB);
        writer.write("return res;");
        writer.write(NL);
        writer.write(TAB);
        writer.write("}");
        writer.write(NL);
    }

    /** */
    private static void getInstance(Writer writer) throws IOException {
        writer.write(TAB);
        writer.write("/** */");
        writer.write(NL);
        writer.write(TAB);
        writer.write("public static " + FACTORY_CLASS + " getInstance() {");
        writer.write(NL);
        writer.write(TAB);
        writer.write(TAB);
        writer.write("return instance;");
        writer.write(NL);
        writer.write(TAB);
        writer.write("}");
        writer.write(NL);
    }

    /** */
    private static void constructor(Map<TypeElement, String> genSerDes, Writer writer) throws IOException {
        writer.write(TAB);
        writer.write("/** */");
        writer.write(NL);
        writer.write(TAB);
        writer.write("private " + FACTORY_CLASS + "() {");
        writer.write(NL);

        for (Map.Entry<TypeElement, String> e : genSerDes.entrySet()) {
            writer.write(TAB);
            writer.write(TAB);
            writer.write("serdes.put(" + e.getKey().getSimpleName() + ".class, new " + simpleName(e.getValue()) + "());");
            writer.write(NL);
        }

        writer.write(TAB);
        writer.write("}");
        writer.write(NL);
    }

    /** */
    private void writeClassHeader(Writer writer, Set<TypeElement> dtoClss) throws IOException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("license.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            PrintWriter out = new PrintWriter(writer);

            String line;

            while ((line = reader.readLine()) != null)
                out.println(line);
        }

        writer.write(NL);
        writer.write("package " + PKG_NAME + ";" + NL + NL);

        for (TypeElement dtoCls : dtoClss)
            writer.write("import " + dtoCls.getQualifiedName() + ";" + NL);

        writer.write("import " + Map.class.getName() + ";" + NL);
        writer.write("import " + HashMap.class.getName() + ";" + NL);
        writer.write("import " + DTO_SERDES_INTERFACE + ";" + NL);
        writer.write("import " + DTO_CLASS + ";" + NL);
        writer.write("import org.apache.ignite.internal.util.typedef.internal.U;" + NL);

        writer.write(NL);
        writer.write(CLS_JAVADOC);
        writer.write(NL);
        writer.write("public class " + FACTORY_CLASS + " {" + NL);
    }

    /** */
    private boolean hasArgumentFields(TypeElement type, TypeMirror argAnnotation) {
        while (type != null) {
            for (Element el: type.getEnclosedElements()) {
                if (el.getKind() != ElementKind.FIELD)
                    continue;

                for (AnnotationMirror am : el.getAnnotationMirrors()) {
                    if (am.getAnnotationType().equals(argAnnotation))
                        return true;
                }
            }

            Element superType = processingEnv.getTypeUtils().asElement(type.getSuperclass());
            type = (TypeElement)superType;
        }

        return false;
    }
}
