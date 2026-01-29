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

package org.apache.ignite.internal.managers.systemview.walker;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.ObjIntConsumer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.JavaFileObject;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Generator for {@code SystemViewRowAttributeWalker} implementations.
 * Used by {@link SystemViewRowAttributeWalkerProcessor} to generate walker classes during compilation.
 */
public class SystemViewRowAttributeWalkerGenerator {
    /** Methods that should be excluded from specific {@code SystemViewRowAttributeWalker}. */
    private static final Set<String> SYS_METHODS = new HashSet<>(Arrays.asList("equals", "hashCode", "toString",
        "getClass"));

    /** Package for {@code SystemViewRowAttributeWalker} implementations. */
    public static final String WALKER_PACKAGE = "org.apache.ignite.internal.managers.systemview.walker.codegen";

    /** */
    public static final String TAB = "    ";

    /** Processing environment. */
    private final ProcessingEnvironment env;

    /** @param env Processing environment. */
    public SystemViewRowAttributeWalkerGenerator(ProcessingEnvironment env) {
        this.env = env;
    }

    /**
     * Generates the walker class for the given view row class and its ordered getter methods.
     *
     * @param cls View row class element.
     * @throws IOException If file creation fails.
     */
    public void generate(TypeElement cls) throws IOException {
        String clsSimple = cls.getSimpleName().toString();
        String walkerClsName = clsSimple + "Walker";
        String walkerQualified = WALKER_PACKAGE + "." + walkerClsName;

        JavaFileObject file = env.getFiler().createSourceFile(walkerQualified);

        try (PrintWriter w = new PrintWriter(file.openWriter())) {
            for (String ln : generateWalker(cls))
                w.println(ln);
        }
    }

    /**
     * Generates {@code SystemViewRowAttributeWalker} implementation.
     *
     * @param clazz Class type to generate {@code SystemViewRowAttributeWalker} for.
     * @return Java source code of the {@code SystemViewRowAttributeWalker} implementation.
     */
    private Collection<String> generateWalker(TypeElement clazz) {
        final List<String> code = new ArrayList<>();
        final Set<String> imports = new TreeSet<>(Comparator.comparing(s -> s.replace(";", "")));

        imports.add("import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;");
        imports.add("import " + clazz.getQualifiedName().toString() + ';');

        String simpleName = clazz.getSimpleName().toString();

        code.add("package " + WALKER_PACKAGE + ";");
        code.add("");
        code.add("");
        code.add("/**");
        code.add(" * This class is generated automatically.");
        code.add(" * ");
        code.add(" * @see org.apache.ignite.internal.managers.systemview.walker.SystemViewRowAttributeWalkerProcessor");
        code.add(" */");
        code.add("public class " + simpleName + "Walker implements SystemViewRowAttributeWalker<" + simpleName + "> {");

        List<String> filtrableAttrs = new ArrayList<>();

        forEachMethod(clazz, (m, i) -> {
            if (m.getAnnotation(Filtrable.class) != null) {
                code.add(TAB + "/** Filter key for attribute \"" + m.getSimpleName() + "\" */");
                code.add(TAB + "public static final String " + m.getSimpleName().toString()
                    .replaceAll("(\\p{Upper})", "_$1")
                    .toUpperCase() + "_FILTER = \"" + m.getSimpleName() + "\";");
                code.add("");

                filtrableAttrs.add(m.getSimpleName().toString());
            }
        });

        if (!filtrableAttrs.isEmpty()) {
            addImport(imports, F.class);
            addImport(imports, List.class);
            addImport(imports, Collections.class);

            code.add(TAB + "/** List of filtrable attributes. */");
            code.add(TAB + "private static final List<String> FILTRABLE_ATTRS = Collections.unmodifiableList(F.asList(");
            code.add(TAB + TAB + '\"' + String.join("\", \"", filtrableAttrs) + '\"');
            code.add(TAB + "));");
            code.add("");
            code.add(TAB + "/** {@inheritDoc} */");
            code.add(TAB + "@Override public List<String> filtrableAttributes() {");
            code.add(TAB + TAB + "return FILTRABLE_ATTRS;");
            code.add(TAB + "}");
            code.add("");
        }

        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public void visitAll(AttributeVisitor v) {");

        forEachMethod(clazz, (m, i) -> {
            String name = m.getSimpleName().toString();

            TypeMirror retClazz = m.getReturnType();

            String line = TAB + TAB;

            if (!retClazz.getKind().isPrimitive() && !qualifiedName(retClazz).startsWith("java.lang"))
                imports.add("import " + qualifiedName(retClazz) + ";");

            line += "v.accept(" + i + ", \"" + name + "\", " + simpleName(retClazz) + ".class);";

            code.add(line);
        });

        code.add(TAB + "}");
        code.add("");
        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public void visitAll(" + simpleName + " row, AttributeWithValueVisitor v) {");

        forEachMethod(clazz, (m, i) -> {
            String name = m.getSimpleName().toString();

            TypeMirror retClazz = m.getReturnType();

            String line = TAB + TAB;

            if (!retClazz.getKind().isPrimitive()) {
                line += "v.accept(" + i + ", \"" + name + "\", " + simpleName(retClazz) + ".class, row."
                    + m.getSimpleName() + "());";
            }
            else if (retClazz.getKind() == TypeKind.BOOLEAN)
                line += "v.acceptBoolean(" + i + ", \"" + name + "\", row." + m.getSimpleName() + "());";
            else if (retClazz.getKind() == TypeKind.CHAR)
                line += "v.acceptChar(" + i + ", \"" + name + "\", row." + m.getSimpleName() + "());";
            else if (retClazz.getKind() == TypeKind.BYTE)
                line += "v.acceptByte(" + i + ", \"" + name + "\", row." + m.getSimpleName() + "());";
            else if (retClazz.getKind() == TypeKind.SHORT)
                line += "v.acceptShort(" + i + ", \"" + name + "\", row." + m.getSimpleName() + "());";
            else if (retClazz.getKind() == TypeKind.INT)
                line += "v.acceptInt(" + i + ", \"" + name + "\", row." + m.getSimpleName() + "());";
            else if (retClazz.getKind() == TypeKind.LONG)
                line += "v.acceptLong(" + i + ", \"" + name + "\", row." + m.getSimpleName() + "());";
            else if (retClazz.getKind() == TypeKind.FLOAT)
                line += "v.acceptFloat(" + i + ", \"" + name + "\", row." + m.getSimpleName() + "());";
            else if (retClazz.getKind() == TypeKind.DOUBLE)
                line += "v.acceptDouble(" + i + ", \"" + name + "\", row." + m.getSimpleName() + "());";

            code.add(line);
        });

        code.add(TAB + "}");
        code.add("");

        final int[] cnt = {0};
        forEachMethod(clazz, (m, i) -> cnt[0]++);

        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public int count() {");
        code.add(TAB + TAB + "return " + cnt[0] + ';');
        code.add(TAB + "}");
        code.add("}");

        code.addAll(2, imports);

        addLicenseHeader(code);

        return code;
    }

    /** Adds import to set imports set. */
    private void addImport(Set<String> imports, Class<?> cls) {
        imports.add("import " + cls.getName().replaceAll("\\$", ".") + ';');
    }

    /**
     * Adds Apache License Header to the source code.
     *
     * @param code Source code.
     */
    private void addLicenseHeader(List<String> code) {
        List<String> lic = new ArrayList<>();

        lic.add("/*");
        lic.add(" * Licensed to the Apache Software Foundation (ASF) under one or more");
        lic.add(" * contributor license agreements.  See the NOTICE file distributed with");
        lic.add(" * this work for additional information regarding copyright ownership.");
        lic.add(" * The ASF licenses this file to You under the Apache License, Version 2.0");
        lic.add(" * (the \"License\"); you may not use this file except in compliance with");
        lic.add(" * the License.  You may obtain a copy of the License at");
        lic.add(" *");
        lic.add(" *      http://www.apache.org/licenses/LICENSE-2.0");
        lic.add(" *");
        lic.add(" * Unless required by applicable law or agreed to in writing, software");
        lic.add(" * distributed under the License is distributed on an \"AS IS\" BASIS,");
        lic.add(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
        lic.add(" * See the License for the specific language governing permissions and");
        lic.add(" * limitations under the License.");
        lic.add(" */");
        lic.add("");

        code.addAll(0, lic);
    }

    /**
     * Iterates each method of the {@code clazz} and call consume {@code c} for it.
     *
     * @param c Method consumer.
     */
    private void forEachMethod(TypeElement clazz, ObjIntConsumer<ExecutableElement> c) {
        List<ExecutableElement> notOrdered = new ArrayList<>();
        List<ExecutableElement> ordered = new ArrayList<>();

        while (clazz != null) {
            for (Element el : clazz.getEnclosedElements()) {
                if (el.getKind() != ElementKind.METHOD)
                    continue;

                ExecutableElement m = (ExecutableElement)el;

                if (m.getModifiers().contains(Modifier.STATIC)
                    || !m.getModifiers().contains(Modifier.PUBLIC)
                    || m.getReturnType().getKind() == TypeKind.VOID)
                    continue;

                if (SYS_METHODS.contains(m.getSimpleName().toString()))
                    continue;

                if (el.getAnnotation(Order.class) != null)
                    ordered.add(m);
                else
                    notOrdered.add(m);
            }

            Element superType = env.getTypeUtils().asElement(clazz.getSuperclass());

            clazz = (TypeElement)superType;
        }

        ordered.sort(Comparator.comparingInt(m -> m.getAnnotation(Order.class).value()));
        notOrdered.sort(Comparator.comparing(m -> m.getSimpleName().toString()));

        for (int i = 0; i < ordered.size(); i++)
            c.accept(ordered.get(i), i);

        for (int i = 0; i < notOrdered.size(); i++)
            c.accept(notOrdered.get(i), i + ordered.size());
    }

    /** */
    private String simpleName(TypeMirror type) {
        if (type.getKind() == TypeKind.DECLARED) {
            DeclaredType declaredType = (DeclaredType)type;

            return declaredType.asElement().getSimpleName().toString();
        }

        return type.toString();
    }

    /** */
    private String qualifiedName(TypeMirror type) {
        if (type.getKind() == TypeKind.DECLARED) {
            DeclaredType declaredType = (DeclaredType)type;
            TypeElement el = (TypeElement)declaredType.asElement();

            return el.getQualifiedName().toString();
        }

        return type.toString();
    }
}
