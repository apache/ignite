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
import javax.lang.model.element.NestingKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

import static org.apache.ignite.internal.MessageSerializerGenerator.NL;
import static org.apache.ignite.internal.MessageSerializerGenerator.TAB;
import static org.apache.ignite.internal.MessageSerializerGenerator.identicalFileIsAlreadyGenerated;
import static org.apache.ignite.internal.idto.IDTOSerializerGenerator.CLS_JAVADOC;
import static org.apache.ignite.internal.idto.IDTOSerializerGenerator.DTO_SERDES_INTERFACE;
import static org.apache.ignite.internal.idto.IDTOSerializerGenerator.simpleName;

/**
 * Generates implementations of {@code IgniteDataTransferObjectSerializer} for all supported classes.
 * Generates factory {@code IDTOSerializerFactory} to get instance of serializer for given class.
 * See, {@code IgniteDataTransferObject#writeExternal(ObjectOutput)} and {@code IgniteDataTransferObject#writeExternal(ObjectInput)} to get
 * insight of using serializers.
 */
@SupportedAnnotationTypes("org.apache.ignite.internal.management.api.Argument")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class IgniteDataTransferObjectProcessor extends AbstractProcessor {
    /** Package for serializers. */
    private static final String FACTORY_PKG_NAME = "org.apache.ignite.internal.codegen.idto";

    /** Base class that every dto must extend. */
    private static final String DTO_CLASS = "org.apache.ignite.internal.dto.IgniteDataTransferObject";

    /**
     * Annotation used in management commands.
     * For now, we restrict set of generated serdes to all management commands argument classes.
     * Because, they strictly follows Ignite codestyle convention.
     * Providing support of all other inheritor of {@code IgniteDataTransferObject} is matter of following improvements.
     */
    private static final String ARG_ANNOTATION = "org.apache.ignite.internal.management.api.Argument";

    /** Factory class name. */
    public static final String FACTORY_CLASS = "IDTOSerializerFactory";

    /** Generated classes. */
    private final Map<TypeElement, String> genSerDes = new HashMap<>();

    /**
     * Processes all classes extending the {@code IgniteDataTransferObject} and generates corresponding serializer code.
     */
    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (roundEnv.errorRaised())
            return true;

        genSerDes.clear();

        roundEnv.getRootElements().forEach(this::generateSingle);

        // IDE recompile only modified classes. Don't want to touch factory in the case no matching classes was recompiled.
        if (genSerDes.isEmpty())
            return true;

        generateFactory(genSerDes);

        return true;
    }

    /**
     * @param el Element to generate code for.
     */
    private void generateSingle(Element el) {
        if (el.getKind() != ElementKind.CLASS)
            return;

        TypeMirror dtoCls = processingEnv.getElementUtils().getTypeElement(DTO_CLASS).asType();
        TypeMirror argAnnotation = processingEnv.getElementUtils().getTypeElement(ARG_ANNOTATION).asType();

        TypeElement clazz = (TypeElement)el;

        // Generate code for inner classes.
        clazz.getEnclosedElements().forEach(this::generateSingle);

        if (!processingEnv.getTypeUtils().isAssignable(clazz.asType(), dtoCls))
            return;

        if (clazz.getModifiers().contains(Modifier.ABSTRACT))
            return;

        if (!clazz.getModifiers().contains(Modifier.PUBLIC))
            return;

        if (clazz.getNestingKind() != NestingKind.TOP_LEVEL && clazz.getNestingKind() != NestingKind.MEMBER)
            return;

        if (!hasArgumentFields(clazz, argAnnotation))
            return;

        try {
            IDTOSerializerGenerator gen = new IDTOSerializerGenerator(processingEnv, clazz);

            if (gen.generate())
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

    /**
     * Generates and writes factory.
     * @param genSerDes Generated serdes classes.
     */
    private void generateFactory(Map<TypeElement, String> genSerDes) {
        try {
            String factoryFQN = FACTORY_PKG_NAME + "." + FACTORY_CLASS;
            String factoryCode = factoryCode(genSerDes);

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
                if (!identicalFileIsAlreadyGenerated(processingEnv, factoryCode, FACTORY_PKG_NAME + "." + FACTORY_CLASS)) {
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
    }

    /**
     * @param genSerDes Generated serdes classes.
     * @return Factory code.
     * @throws IOException In case of error.
     */
    private String factoryCode(Map<TypeElement, String> genSerDes) throws IOException {
        try (Writer writer = new StringWriter()) {
            writeClassHeader(writer, genSerDes);

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
            writer.write("private final Map<Class<? extends IgniteDataTransferObject>, " + simpleName(DTO_SERDES_INTERFACE) + "> serdes " +
                "= new HashMap<>();");
            writer.write(NL);
            writer.write(NL);

            constructor(writer, genSerDes);
            writer.write(NL);

            getInstance(writer);
            writer.write(NL);

            serializer(writer);

            writer.write("}");
            writer.write(NL);

            return writer.toString();
        }
    }

    /**
     * Generates class header.
     *
     * @param writer Writer to write code to.
     * @param dtoClss DTO classes to import.
     * @throws IOException In case of error.
     */
    private void writeClassHeader(Writer writer, Map<TypeElement, String> dtoClss) throws IOException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("license.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            PrintWriter out = new PrintWriter(writer);

            String line;

            while ((line = reader.readLine()) != null)
                out.println(line);
        }

        writer.write(NL);
        writer.write("package " + FACTORY_PKG_NAME + ";" + NL + NL);

        for (Map.Entry<TypeElement, String> e : dtoClss.entrySet()) {
            writer.write("import " + e.getKey().getQualifiedName() + ";" + NL);
            writer.write("import " + e.getValue() + ";" + NL);
        }

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

    /**
     * Generates static {@code getInstance} method.
     *
     * @param writer Writer to write code to.
     * @throws IOException In case of error.
     */
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

    /**
     * Generates private constructor.
     *
     * @param writer Writer to write code to.
     * @param genSerDes Serdes to support in factory.
     * @throws IOException In case of error.
     */
    private static void constructor(Writer writer, Map<TypeElement, String> genSerDes) throws IOException {
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

    /**
     * Generates method to get serializer from factory.
     *
     * @param writer Writer to write code to.
     * @throws IOException In case of error.
     */
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
        writer.write("if (res == null) {");
        writer.write(NL);
        writer.write(TAB);
        writer.write(TAB);
        writer.write(TAB);
        // IDE can invoke partial recompile during development.
        // In this case there will be only part (one) of serdes in the map initially.
        // We want to be able to load serializer dynamically if it is missing in the map but class file itself is presented in classpath.
        // Other case to do it custom commands.
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

    /**
     * @param type Type to analyze.
     * @param argAnnotation Annotation to find.
     * @return {@code True} if type has fields annotated with the {@code argAnnotation}, {@code false} otherwise.
     */
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

            type = (TypeElement)processingEnv.getTypeUtils().asElement(type.getSuperclass());
        }

        return false;
    }
}
