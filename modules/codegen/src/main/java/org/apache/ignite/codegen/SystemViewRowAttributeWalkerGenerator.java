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

package org.apache.ignite.codegen;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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
import org.apache.ignite.internal.managers.systemview.walker.Filtrable;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.SystemViewLocal;
import org.apache.ignite.spi.systemview.jmx.SystemViewMBean;
import org.apache.ignite.spi.systemview.view.BinaryMetadataView;
import org.apache.ignite.spi.systemview.view.CacheGroupView;
import org.apache.ignite.spi.systemview.view.CachePagesListView;
import org.apache.ignite.spi.systemview.view.CacheView;
import org.apache.ignite.spi.systemview.view.ClientConnectionView;
import org.apache.ignite.spi.systemview.view.ClusterNodeView;
import org.apache.ignite.spi.systemview.view.ComputeJobView;
import org.apache.ignite.spi.systemview.view.ComputeTaskView;
import org.apache.ignite.spi.systemview.view.ContinuousQueryView;
import org.apache.ignite.spi.systemview.view.MetastorageView;
import org.apache.ignite.spi.systemview.view.PagesListView;
import org.apache.ignite.spi.systemview.view.PartitionStateView;
import org.apache.ignite.spi.systemview.view.ScanQueryView;
import org.apache.ignite.spi.systemview.view.ServiceView;
import org.apache.ignite.spi.systemview.view.SqlIndexView;
import org.apache.ignite.spi.systemview.view.SqlQueryHistoryView;
import org.apache.ignite.spi.systemview.view.SqlQueryView;
import org.apache.ignite.spi.systemview.view.SqlSchemaView;
import org.apache.ignite.spi.systemview.view.SqlTableColumnView;
import org.apache.ignite.spi.systemview.view.SqlTableView;
import org.apache.ignite.spi.systemview.view.SqlViewColumnView;
import org.apache.ignite.spi.systemview.view.SqlViewView;
import org.apache.ignite.spi.systemview.view.StripedExecutorTaskView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.apache.ignite.spi.systemview.view.TransactionView;

import static org.apache.ignite.codegen.MessageCodeGenerator.DFLT_SRC_DIR;
import static org.apache.ignite.codegen.MessageCodeGenerator.INDEXING_SRC_DIR;

/**
 * Application for code generation of {@link SystemViewRowAttributeWalker}.
 * Usage: simply run main method from Ignite source folder(using IDE or other way).
 * Generated code used in {@link SystemView}.
 *
 * @see SystemViewMBean
 * @see SystemViewLocal
 */
public class SystemViewRowAttributeWalkerGenerator {
    /** Methods that should be excluded from specific {@link SystemViewRowAttributeWalker}. */
    private static final Set<String> SYS_METHODS = new HashSet<>(Arrays.asList("equals", "hashCode", "toString",
        "getClass"));

    /** Package for {@link SystemViewRowAttributeWalker} implementations. */
    public static final String WALKER_PACKAGE = "org.apache.ignite.internal.managers.systemview.walker";

    /** */
    public static final String TAB = "    ";

    /**
     * @throws Exception If generation failed.
     */
    public static void main(String[] args) throws Exception {
        SystemViewRowAttributeWalkerGenerator gen = new SystemViewRowAttributeWalkerGenerator();

        gen.generateAndWrite(CacheGroupView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(CacheView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ServiceView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ComputeTaskView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ComputeJobView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ClientConnectionView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(TransactionView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ContinuousQueryView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ClusterNodeView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ScanQueryView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(SqlQueryView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(SqlQueryHistoryView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(StripedExecutorTaskView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PagesListView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(CachePagesListView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(PartitionStateView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(BinaryMetadataView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(MetastorageView.class, DFLT_SRC_DIR);

        gen.generateAndWrite(SqlSchemaView.class, INDEXING_SRC_DIR);
        gen.generateAndWrite(SqlTableView.class, INDEXING_SRC_DIR);
        gen.generateAndWrite(SqlViewView.class, INDEXING_SRC_DIR);
        gen.generateAndWrite(SqlIndexView.class, INDEXING_SRC_DIR);
        gen.generateAndWrite(SqlTableColumnView.class, INDEXING_SRC_DIR);
        gen.generateAndWrite(SqlViewColumnView.class, INDEXING_SRC_DIR);
    }

    /**
     * Generates {@link SystemViewRowAttributeWalker} implementation and write it to the file.
     *
     * @param clazz Class to geneare {@link SystemViewRowAttributeWalker} for.
     * @param srcRoot Source root folder.
     * @param <T> type of the row.
     * @throws IOException If generation failed.
     */
    private <T> void generateAndWrite(Class<T> clazz, String srcRoot) throws IOException {
        File walkerClass = new File(srcRoot + '/' + WALKER_PACKAGE.replaceAll("\\.", "/") + '/' +
            clazz.getSimpleName() + "Walker.java");

        Collection<String> code = generate(clazz);

        walkerClass.createNewFile();

        try (FileWriter writer = new FileWriter(walkerClass)) {
            for (String line : code) {
                writer.write(line);
                writer.write('\n');
            }
        }
    }

    /**
     * Generates {@link SystemViewRowAttributeWalker} implementation.
     *
     * @param clazz Class to geneare {@link SystemViewRowAttributeWalker} for.
     * @param <T> type of the row.
     * @return Java source code of the {@link SystemViewRowAttributeWalker} implementation.
     */
    private <T> Collection<String> generate(Class<T> clazz) {
        final List<String> code = new ArrayList<>();
        final Set<String> imports = new TreeSet<>();

        addImport(imports, SystemViewRowAttributeWalker.class);
        addImport(imports, clazz);

        String simpleName = clazz.getSimpleName();

        code.add("package " + WALKER_PACKAGE + ";");
        code.add("");
        code.add("");
        code.add("/**");
        code.add(" * Generated by {@code " + SystemViewRowAttributeWalkerGenerator.class.getName() + "}.");
        code.add(" * {@link " + simpleName + "} attributes walker.");
        code.add(" * ");
        code.add(" * @see " + simpleName);
        code.add(" */");
        code.add("public class " + simpleName + "Walker implements SystemViewRowAttributeWalker<" + simpleName + "> {");

        List<String> filtrableAttrs = new ArrayList<>();

        forEachMethod(clazz, (m, i) -> {
            if (m.getAnnotation(Filtrable.class) != null) {
                code.add(TAB + "/** Filter key for attribute \"" + m.getName() + "\" */");
                code.add(TAB + "public static final String " + m.getName()
                    .replaceAll("(\\p{Upper})", "_$1")
                    .toUpperCase() + "_FILTER = \"" + m.getName() + "\";");
                code.add("");

                filtrableAttrs.add(m.getName());
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
            String name = m.getName();

            Class<?> retClazz = m.getReturnType();

            String line = TAB + TAB;

            if (!retClazz.isPrimitive() && !retClazz.getName().startsWith("java.lang"))
                addImport(imports, retClazz);

            line += "v.accept(" + i + ", \"" + name + "\", " + retClazz.getSimpleName() + ".class);";

            code.add(line);
        });

        code.add(TAB + "}");
        code.add("");
        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public void visitAll(" + simpleName + " row, AttributeWithValueVisitor v) {");

        forEachMethod(clazz, (m, i) -> {
            String name = m.getName();

            Class<?> retClazz = m.getReturnType();

            String line = TAB + TAB;

            if (!retClazz.isPrimitive()) {
                line += "v.accept(" + i + ", \"" + name + "\", " + retClazz.getSimpleName() + ".class, row." + m.getName() + "());";
            }
            else if (retClazz == boolean.class)
                line += "v.acceptBoolean(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == char.class)
                line += "v.acceptChar(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == byte.class)
                line += "v.acceptByte(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == short.class)
                line += "v.acceptShort(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == int.class)
                line += "v.acceptInt(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == long.class)
                line += "v.acceptLong(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == float.class)
                line += "v.acceptFloat(" + i + ", \"" + name + "\", row." + m.getName() + "());";
            else if (retClazz == double.class)
                line += "v.acceptDouble(" + i + ", \"" + name + "\", row." + m.getName() + "());";

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
    private void forEachMethod(Class<?> clazz, ObjIntConsumer<Method> c) {
        Method[] methods = clazz.getMethods();

        List<Method> notOrdered = new ArrayList<>();
        List<Method> ordered = new ArrayList<>();

        for (int i = 0; i < methods.length; i++) {
            Method m = methods[i];

            if (Modifier.isStatic(m.getModifiers()))
                continue;

            if (SYS_METHODS.contains(m.getName()))
                continue;

            Class<?> retClazz = m.getReturnType();

            if (retClazz == void.class)
                continue;

            if (m.getAnnotation(Order.class) != null)
                ordered.add(m);
            else
                notOrdered.add(m);
        }

        Collections.sort(ordered, Comparator.comparingInt(m -> m.getAnnotation(Order.class).value()));
        Collections.sort(notOrdered, Comparator.comparing(Method::getName));

        for (int i = 0; i < ordered.size(); i++)
            c.accept(ordered.get(i), i);

        for (int i = 0; i < notOrdered.size(); i++)
            c.accept(notOrdered.get(i), i + ordered.size());
    }
}
