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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.ObjIntConsumer;
import org.apache.ignite.internal.processors.metric.list.MonitoringList;
import org.apache.ignite.internal.processors.metric.list.MonitoringRow;
import org.apache.ignite.internal.processors.metric.list.view.SqlIndexView;
import org.apache.ignite.internal.processors.metric.list.view.SqlSchemaView;
import org.apache.ignite.internal.processors.metric.list.view.SqlTableView;
import org.apache.ignite.spi.metric.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.CacheGroupView;
import org.apache.ignite.spi.metric.list.CacheView;
import org.apache.ignite.spi.metric.list.ClientConnectionView;
import org.apache.ignite.spi.metric.list.ContinuousQueryView;
import org.apache.ignite.spi.metric.list.QueryView;
import org.apache.ignite.spi.metric.list.ServiceView;
import org.apache.ignite.spi.metric.list.TransactionView;

import static org.apache.ignite.codegen.MessageCodeGenerator.DFLT_SRC_DIR;
import static org.apache.ignite.codegen.MessageCodeGenerator.INDEXING_SRC_DIR;
import static org.apache.ignite.codegen.MessageCodeGenerator.TAB;

/**
 * Generator of {@link MonitoringRowAttributeWalker}.
 * This code used in {@link MonitoringList}.
 */
public class MonitoringRowAttributeWalkerGenerator {
    /** */
    private static final Set<String> SYSTEM_METHODS = new HashSet<>(Arrays.asList("equals", "hashCode", "toString",
        "getClass"));

    /**
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        MonitoringRowAttributeWalkerGenerator gen = new MonitoringRowAttributeWalkerGenerator();

        gen.generateAndWrite(CacheGroupView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(CacheView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ClientConnectionView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ContinuousQueryView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(QueryView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(ServiceView.class, DFLT_SRC_DIR);
        gen.generateAndWrite(TransactionView.class, DFLT_SRC_DIR);

        gen.generateAndWrite(SqlIndexView.class, INDEXING_SRC_DIR);
        gen.generateAndWrite(SqlSchemaView.class, INDEXING_SRC_DIR);
        gen.generateAndWrite(SqlTableView.class, INDEXING_SRC_DIR);
    }

    /**
     * @param clazz
     * @param srcRoot
     * @param <T>
     * @throws IOException
     */
    private <T extends MonitoringRow<?>> void generateAndWrite(Class<T> clazz, String srcRoot) throws IOException {
        File walkerClass = new File(srcRoot + '/' + clazz.getPackage().getName().replaceAll("\\.", "/") + "/walker/" +
                clazz.getSimpleName() + "Walker.java");

        Collection<String> code = generate(clazz);

        walkerClass.createNewFile();

        try (FileWriter writer = new FileWriter(walkerClass)) {
        for (String line : code)
            writer.write(line + '\n');
        }
    }

    /**
     * @param clazz
     * @param <T>
     * @return
     */
    private <T extends MonitoringRow<?>> Collection<String> generate(Class<T> clazz) {
        final List<String> code = new ArrayList<>();
        final Set<String> imports = new TreeSet<>();

        imports.add("import " + MonitoringRowAttributeWalker.class.getName() + ';');
        imports.add("import " + clazz.getName() + ';');

        String simpleName = clazz.getSimpleName();

        code.add("package " + clazz.getPackage().getName() + ".walker;");
        code.add("");
        code.add("");
        code.add("/** */");
        code.add("public class " + simpleName + "Walker implements MonitoringRowAttributeWalker<" + simpleName + "> {");
        code.add("");
        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public void visitAll(AttributeVisitor v) {");

        forEachMethod(clazz, (m, i) -> {
            String name = m.getName();

            Class<?> retClazz = m.getReturnType();

            String line = TAB + TAB;

            if (!retClazz.isPrimitive()) {
                if (!retClazz.getName().startsWith("java.lang"))
                    imports.add("import " + retClazz.getName() + ';');

                line += "v.accept(" + i + ", \"" + name + "\", " + retClazz.getSimpleName() + ".class);";
            } else if (retClazz == boolean.class)
                line += "v.acceptBoolean(" + i + ", \"" + name + "\");";
            else if (retClazz == char.class)
                line += "v.acceptChar(" + i + ", \"" + name + "\");";
            else if (retClazz == byte.class)
                line += "v.acceptByte(" + i + ", \"" + name + "\");";
            else if (retClazz == short.class)
                line += "v.acceptShort(" + i + ", \"" + name + "\");";
            else if (retClazz == int.class)
                line += "v.acceptInt(" + i + ", \"" + name + "\");";
            else if (retClazz == long.class)
                line += "v.acceptLong(" + i + ", \"" + name + "\");";
            else if (retClazz == float.class)
                line += "v.acceptFloat(" + i + ", \"" + name + "\");";
            else if (retClazz == double.class)
                line += "v.acceptDouble(" + i + ", \"" + name + "\");";

            code.add(line);
        });

        code.add(TAB + "}");
        code.add("");
        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public void visitAllWithValues(" + simpleName + " row, AttributeWithValueVisitor v) {");

        forEachMethod(clazz, (m, i) -> {
            String name = m.getName();

            Class<?> retClazz = m.getReturnType();

            String line = TAB + TAB;

            if (!retClazz.isPrimitive()) {
                line += "v.accept(" + i + ", \"" + name + "\", " + retClazz.getSimpleName() + ".class, row." + m.getName() + "());";
            } else if (retClazz == boolean.class)
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
        code.add("");

        code.addAll(2, imports);

        addLicenseHeader(code);

        return code;
    }

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
     * @param c Method consumer.
     */
    private void forEachMethod(Class<?> clazz, ObjIntConsumer<Method> c) {
        Method[] methods = clazz.getMethods();

        int idx = 0;

        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];

            if (Modifier.isStatic(method.getModifiers()))
                continue;

            if (SYSTEM_METHODS.contains(method.getName()))
                continue;

            Class<?> retClazz = method.getReturnType();

            if (retClazz == void.class)
                continue;

            c.accept(method, idx++);
        }
    }
}
