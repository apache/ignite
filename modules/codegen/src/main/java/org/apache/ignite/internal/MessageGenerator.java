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
import java.io.Writer;
import java.util.List;
import java.util.TreeSet;
import javax.annotation.processing.FilerException;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;

/** Base class for message code generators ({@link MessageSerializerGenerator}, {@link MessageMarshallerGenerator}). */
public abstract class MessageGenerator {
    /** */
    public static final String EMPTY = "";

    /** */
    public static final String NL = System.lineSeparator();

    /** */
    public static final String TAB = "    ";

    /** */
    public static final String METHOD_JAVADOC = "/** */";

    /** */
    public static final String CLS_JAVADOC = "/**" + NL +
        " * This class is generated automatically." + NL +
        " *" + NL +
        " * @see org.apache.ignite.internal.MessageProcessor" + NL +
        " */";

    /** */
    final ProcessingEnvironment env;

    /** Collection of message-specific imports. */
    final java.util.Set<String> imports = new TreeSet<>();

    /** Stored type of the message being processed. */
    TypeElement type;

    /** */
    int indent;

    /** */
    MessageGenerator(ProcessingEnvironment env) {
        this.env = env;
    }

    /**
     * Generates and writes the source file for the given message type.
     * Template method: subclasses implement {@link #generateBody} and {@link #buildClassCode}.
     */
    final void generate(TypeElement type, List<VariableElement> fields) throws Exception {
        assert this.type == null : "Message" + typeSuffix() + " generator isn't stateless and is supposed to be single-use.";

        if (shouldSkip(type))
            return;

        this.type = type;

        generateBody(fields);

        String clsName = type.getSimpleName() + typeSuffix();
        String fqnClsName = env.getElementUtils().getPackageOf(type) + "." + clsName;
        String code = buildClassCode(clsName);

        try {
            JavaFileObject file = env.getFiler().createSourceFile(fqnClsName);

            try (Writer writer = file.openWriter()) {
                writer.append(code);
                writer.flush();
            }
        }
        catch (FilerException e) {
            if (!identicalFileIsAlreadyGenerated(env, code, fqnClsName)) {
                env.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "Message" + typeSuffix() + " " + clsName + " is already generated. Try 'mvn clean install' to fix the issue.");

                throw e;
            }
        }
    }

    /** @return Class name suffix: {@code "Serializer"} or {@code "Marshaller"}. */
    abstract String typeSuffix();

    /**
     * @return {@code true} if no source file should be generated for the given message type.
     * Default: {@code false} (generate for all types).
     */
    boolean shouldSkip(TypeElement type) {
        return false;
    }

    /**
     * Generates the body of the class by populating internal state (e.g. write/read or marshal/unmarshal lists).
     * Called before {@link #buildClassCode}.
     */
    abstract void generateBody(List<VariableElement> fields) throws Exception;

    /** Generates and returns the complete source code for the generated class. */
    abstract String buildClassCode(String clsName) throws IOException;

    /** Writes the Apache license header to the given writer. */
    void writeLicense(Writer writer) throws IOException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("license.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            PrintWriter out = new PrintWriter(writer);
            String line;

            while ((line = reader.readLine()) != null)
                out.println(line);
        }
    }

    /**
     * Writes the common class header: license, package declaration, imports, Javadoc, and class declaration line.
     * Callers must have already populated {@link #imports} with all required imports before calling this method.
     *
     * @param writer Target writer.
     * @param interfaceName Simple name of the implemented interface (e.g. {@code "MessageSerializer"}).
     * @param clsName Generated class name (e.g. {@code "FooSerializer"}).
     */
    void writeClassHeader(Writer writer, String interfaceName, String clsName) throws IOException {
        writeLicense(writer);

        writer.write(NL);
        writer.write("package " + env.getElementUtils().getPackageOf(type) + ";" + NL + NL);

        for (String imp : imports)
            writer.write("import " + imp + ";" + NL);

        writer.write(NL);
        writer.write(CLS_JAVADOC);
        writer.write(NL);
        writer.write("public class " + clsName + " implements " + interfaceName + "<" + simpleNameWithGeneric(type) + ">");
    }

    /** */
    String indentedLine(String format, Object... args) {
        SB sb = new SB();

        for (int i = 0; i < indent; i++)
            sb.a(TAB);

        sb.a(String.format(format, args));

        return sb.toString();
    }

    /** */
    String simpleNameWithGeneric(TypeElement te) {
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

    /** */
    boolean assignableFrom(TypeMirror type, TypeMirror superType) {
        return env.getTypeUtils().isAssignable(type, superType);
    }

    /** */
    TypeMirror type(String clazz) {
        Elements elementUtils = env.getElementUtils();
        TypeElement typeElement = elementUtils.getTypeElement(clazz);
        return typeElement != null ? typeElement.asType() : null;
    }

    /** */
    TypeMirror erasedType(TypeMirror type) {
        return env.getTypeUtils().erasure(type);
    }

    /** */
    String fieldAccessor(VariableElement field) {
        return "msg." + field.getSimpleName().toString();
    }

    /** @return {@code true} if a file with identical content is already generated (e.g. during IDE incremental build). */
    public static boolean identicalFileIsAlreadyGenerated(ProcessingEnvironment env, String srcCode, String fqnClsName) {
        try {
            String fileName = fqnClsName.replace('.', '/') + ".java";
            FileObject prevFile = env.getFiler().getResource(StandardLocation.SOURCE_OUTPUT, "", fileName);

            String prevFileContent;
            try (Reader r = prevFile.openReader(true)) {
                prevFileContent = content(r);
            }

            if (prevFileContent.contentEquals(srcCode))
                return true;
        }
        catch (Exception ignoredAttemptToGetExistingFile) {
            // We have some other problem, not an existing file.
        }

        return false;
    }

    /** */
    static String content(Reader reader) throws IOException {
        BufferedReader br = new BufferedReader(reader);
        StringBuilder sb = new StringBuilder();
        String line;

        while ((line = br.readLine()) != null)
            sb.append(line).append(NL);

        // Delete last line separator.
        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
    }
}
