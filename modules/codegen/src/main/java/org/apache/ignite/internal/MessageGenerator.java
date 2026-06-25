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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import javax.annotation.processing.FilerException;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;

/** Base class for message code generators ({@link MessageSerializerGenerator}, {@link MessageMarshallerGenerator}). */
public abstract class MessageGenerator {
    /** Blank separator line in generated code. */
    public static final String EMPTY = "";

    public static final String NL = System.lineSeparator();

    public static final String TAB = "    ";

    /** Javadoc stub emitted on every generated {@code @Override} method. */
    public static final String METHOD_JAVADOC = "/** */";

    /** Javadoc block emitted on every generated class. */
    public static final String CLS_JAVADOC = "/**" + NL +
        " * This class is generated automatically." + NL +
        " *" + NL +
        " * @see org.apache.ignite.internal.MessageProcessor" + NL +
        " */";

    /** */
    final ProcessingEnvironment env;

    /** */
    final java.util.Set<String> imports = new TreeSet<>();

    /** */
    TypeElement type;

    /** */
    int indent;

    MessageGenerator(ProcessingEnvironment env) {
        this.env = env;
    }

    /** Generates and writes the source file for {@code type}; skipped when {@link #shouldSkip} returns {@code true}.
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
                env.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "Message" + typeSuffix() + " " + clsName + " is already generated. Try 'mvn clean install' to fix the issue.");

                throw e;
            }
        }
    }

    /** @return Class name suffix: {@code "Serializer"} or {@code "Marshaller"}. */
    abstract String typeSuffix();

    /** @return {@code true} if no file should be generated for this type; default is {@code false}. */
    boolean shouldSkip(TypeElement type) {
        return false;
    }

    /** Populates internal state (method body lines etc.) from {@code fields}; called before {@link #buildClassCode}.
     */
    abstract void generateBody(List<VariableElement> fields) throws Exception;

    /** Generates and returns the complete source code for the generated class. */
    abstract String buildClassCode(String clsName) throws IOException;

    /** */
    void writeLicense(Writer writer) throws IOException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("license.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            PrintWriter out = new PrintWriter(writer);
            String line;

            while ((line = reader.readLine()) != null)
                out.println(line);
        }
    }

    /** Writes license, package, imports, javadoc, and class declaration; {@link #imports} must be populated before calling. */
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

    /** @return {@code format} formatted with {@code args}, prefixed with {@link #indent} tabs. */
    String indentedLine(String format, Object... args) {
        SB sb = new SB();

        for (int i = 0; i < indent; i++)
            sb.a(TAB);

        sb.a(String.format(format, args));

        return sb.toString();
    }

    /** @return simple name of {@code te} with {@code <?, ?>} wildcard type arguments appended when parameterised. */
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

    /** @return the {@link TypeMirror} for the fully-qualified {@code clazz}, or {@code null} if not on classpath. */
    TypeMirror type(String clazz) {
        Elements elementUtils = env.getElementUtils();
        TypeElement typeElement = elementUtils.getTypeElement(clazz);
        return typeElement != null ? typeElement.asType() : null;
    }

    /** */
    TypeMirror erasedType(TypeMirror type) {
        return env.getTypeUtils().erasure(type);
    }

    /** @return {@code "msg.<fieldName>"} accessor expression for {@code field}. */
    String fieldAccessor(VariableElement field) {
        return "msg." + field.getSimpleName().toString();
    }

    /** Returns all fields declared directly on {@link #type}, in declaration order. */
    protected Map<String, VariableElement> enclosedFields() {
        Map<String, VariableElement> result = new LinkedHashMap<>();

        for (VariableElement f : ElementFilter.fieldsIn(type.getEnclosedElements()))
            result.put(f.getSimpleName().toString(), f);

        return result;
    }

    /** Appends {@code block} to {@code body}, inserting a blank-line separator when {@code body} is non-empty. */
    protected static void appendBlock(List<String> body, List<String> block) {
        if (!body.isEmpty())
            body.add(EMPTY);

        body.addAll(block);
    }

    /** @return {@code true} if a file with identical content is already generated (e.g. during incremental build). */
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

        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
    }
}
