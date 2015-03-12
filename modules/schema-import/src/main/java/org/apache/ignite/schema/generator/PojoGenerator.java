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

package org.apache.ignite.schema.generator;

import org.apache.ignite.schema.model.*;
import org.apache.ignite.schema.ui.*;

import java.io.*;
import java.text.*;
import java.util.*;

import static org.apache.ignite.schema.ui.MessageBox.Result.*;

/**
 * POJO generator for key and value classes.
 */
public class PojoGenerator {
    /** */
    private static final String TAB = "    ";
    /** */
    private static final String TAB2 = TAB + TAB;
    /** */
    private static final String TAB3 = TAB + TAB + TAB;

    /**
     * Add line to source code without indent.
     *
     * @param src Source code.
     * @param line Code line.
     */
    private static void add0(Collection<String> src, String line) {
        src.add(line);
    }

    /**
     * Add line to source code with one indent.
     *
     * @param src Source code.
     * @param line Code line.
     */
    private static void add1(Collection<String> src, String line) {
        src.add(TAB + line);
    }

    /**
     * Add line to source code with two indents.
     *
     * @param src Source code.
     * @param line Code line.
     */
    private static void add2(Collection<String> src, String line) {
        src.add(TAB2 + line);
    }

    /**
     * Add line to source code with two indents.
     *
     * @param src Source code.
     * @param fmt Code line with format placeholders.
     * @param args Format arguments.
     */
    private static void add2Fmt(Collection<String> src, String fmt, Object... args) {
        add2(src, String.format(fmt, args));
    }

    /**
     * Add line to source code with three indents.
     *
     * @param src Source code.
     * @param line Code line.
     */
    private static void add3(Collection<String> src, String line) {
        src.add(TAB3 + line);
    }

    /**
     * @param str Source string.
     * @return String with first letters in upper case.
     */
    private static String capitalizeFirst(String str) {
        return Character.toUpperCase(str.charAt(0)) + str.substring(1);
    }

    /**
     * @param field POJO field descriptor.
     * @return Field java type name.
     */
    private static String javaTypeName(PojoField field) {
        String javaTypeName = field.javaTypeName();

        return javaTypeName.startsWith("java.lang.") ? javaTypeName.substring(10) : javaTypeName;
    }

    /**
     * Generate java class code.
     *
     * @param pojo POJO descriptor.
     * @param key {@code true} if key class should be generated.
     * @param pkg Package name.
     * @param pkgFolder Folder where to save generated class.
     * @param constructor {@code true} if empty and full constructors should be generated.
     * @param includeKeys {@code true} if key fields should be included into value class.
     * @param askOverwrite Callback to ask user to confirm file overwrite.
     * @throws IOException If failed to write generated code into file.
     */
    private static void generateCode(PojoDescriptor pojo, boolean key, String pkg, File pkgFolder,
        boolean constructor, boolean includeKeys, ConfirmCallable askOverwrite) throws IOException {
        String type = key ? pojo.keyClassName() : pojo.valueClassName();

        File out = new File(pkgFolder, type + ".java");

        if (out.exists()) {
            MessageBox.Result choice = askOverwrite.confirm(out.getName());

            if (CANCEL == choice)
                throw new IllegalStateException("POJO generation was canceled!");

            if (NO == choice || NO_TO_ALL == choice)
                return;
        }

        Collection<String> src = new ArrayList<>(256);

        // License.
        add0(src, "/*");
        add0(src, " * Licensed to the Apache Software Foundation (ASF) under one or more");
        add0(src, " * contributor license agreements.  See the NOTICE file distributed with");
        add0(src, " * this work for additional information regarding copyright ownership.");
        add0(src, " * The ASF licenses this file to You under the Apache License, Version 2.0");
        add0(src, " * (the \"License\"); you may not use this file except in compliance with");
        add0(src, " * the License.  You may obtain a copy of the License at");
        add0(src, " *");
        add0(src, " *      http://www.apache.org/licenses/LICENSE-2.0");
        add0(src, " *");
        add0(src, " * Unless required by applicable law or agreed to in writing, software");
        add0(src, " * distributed under the License is distributed on an \"AS IS\" BASIS,");
        add0(src, " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
        add0(src, " * See the License for the specific language governing permissions and");
        add0(src, " * limitations under the License.");
        add0(src, " */");
        add0(src, "");

        // Package.
        add0(src, "package " + pkg + ";");
        add0(src, "");

        // Imports.
        add0(src, "import java.io.*;");
        add0(src, "");

        // Class.
        add0(src, "/**");
        add0(src, " * " + type + " definition.");
        add0(src, " *");
        add0(src, " * Code generated by Apache Ignite Schema Import utility: " + new SimpleDateFormat("MM/dd/yyyy").format(new Date()) + ".");
        add0(src, " */");
        add0(src, "public class " + type + " implements Serializable {");

        add1(src, "/** */");
        add1(src, "private static final long serialVersionUID = 0L;");
        add0(src, "");

        Collection<PojoField> fields = key ? pojo.keyFields() : pojo.valueFields(includeKeys);

        // Generate fields declaration.
        for (PojoField field : fields) {
            String fldName = field.javaName();

            add1(src, "/** Value for " + fldName + ". */");

            if (key && field.affinityKey())
                add1(src, "@CacheAffinityKeyMapped");

            add1(src, "private " + javaTypeName(field) + " " + fldName + ";");
            add0(src, "");
        }

        // Generate constructors.
        if (constructor) {
            add1(src, "/**");
            add1(src, " * Empty constructor.");
            add1(src, " */");
            add1(src, "public " + type + "() {");
            add2(src, "// No-op.");
            add1(src, "}");

            add0(src, "");

            add1(src, "/**");
            add1(src, " * Full constructor.");
            add1(src, " */");
            add1(src, "public " + type + "(");

            Iterator<PojoField> it = fields.iterator();

            while (it.hasNext()) {
                PojoField field = it.next();

                add2(src, javaTypeName(field) + " " + field.javaName() + (it.hasNext() ? "," : ""));
            }
            add1(src, ") {");

            for (PojoField field : fields)
                add2Fmt(src, "this.%1$s = %1$s;", field.javaName());

            add1(src, "}");
            add0(src, "");
        }

        // Generate getters and setters methods.
        for (PojoField field : fields) {
            String fldName = field.javaName();

            String fldType = javaTypeName(field);

            String mtdName = capitalizeFirst(fldName);

            add1(src, "/**");
            add1(src, " * Gets " + fldName + ".");
            add1(src, " *");
            add1(src, " * @return Value for " + fldName + ".");
            add1(src, " */");
            add1(src, "public " + fldType + " get" + mtdName + "() {");
            add2(src, "return " + fldName + ";");
            add1(src, "}");
            add0(src, "");

            add1(src, "/**");
            add1(src, " * Sets " + fldName + ".");
            add1(src, " *");
            add1(src, " * @param " + fldName + " New value for " + fldName + ".");
            add1(src, " */");
            add1(src, "public void set" + mtdName + "(" + fldType + " " + fldName + ") {");
            add2(src, "this." + fldName + " = " + fldName + ";");
            add1(src, "}");
            add0(src, "");
        }

        // Generate equals() method.
        add1(src, "/** {@inheritDoc} */");
        add1(src, "@Override public boolean equals(Object o) {");
        add2(src, "if (this == o)");
        add3(src, "return true;");
        add0(src, "");

        add2(src, "if (!(o instanceof " + type + "))");
        add3(src, "return false;");
        add0(src, "");

        add2Fmt(src, "%1$s that = (%1$s)o;", type);

        for (PojoField field : fields) {
            add0(src, "");

            String javaName = field.javaName();

            if (field.primitive()) {
                switch (field.javaTypeName()) {
                    case "float":
                        add2Fmt(src, "if (Float.compare(%1$s, that.%1$s) != 0)", javaName);
                        break;

                    case "double":
                        add2Fmt(src, "if (Double.compare(%1$s, that.%1$s) != 0)", javaName);
                        break;

                    default:
                        add2Fmt(src, "if (%1$s != that.%1$s)", javaName);
                }
            }
            else
                add2Fmt(src, "if (%1$s != null ? !%1$s.equals(that.%1$s) : that.%1$s != null)", javaName);

            add3(src, "return false;");
        }

        add0(src, "");
        add2(src, "return true;");
        add1(src, "}");
        add0(src, "");

        // Generate hashCode() method.
        add1(src, "/** {@inheritDoc} */");
        add1(src, "@Override public int hashCode() {");

        List<String> hash = new ArrayList<>(fields.size() * 2);

        boolean first = true;
        boolean tempVar = false;

        for (PojoField field : fields) {
            String javaName = field.javaName();

            if (!first)
                add0(hash, "");

            if (field.primitive()) {
                switch (field.javaTypeName()) {
                    case "boolean":
                        add2Fmt(hash, first ? "int res = %s ? 1 : 0;" : "res = 31 * res + (%s ? 1 : 0);", javaName);
                        break;

                    case "byte":
                    case "short":
                        add2Fmt(hash, first ? "int res = (int)%s;" : "res = 31 * res + (int)%s;", javaName);
                        break;

                    case "int":
                        add2Fmt(hash, first ? "int res = %s;" : "res = 31 * res + %s;", javaName);
                        break;

                    case "long":
                        add2Fmt(hash, first
                            ? "int res = (int)(%1$s ^ (%1$s >>> 32));"
                            : "res = 31 * res + (int)(%1$s ^ (%1$s >>> 32));", javaName);
                        break;

                    case "float":
                        add2Fmt(hash, first
                            ? "int res = %1$s != +0.0f ? Float.floatToIntBits(%1$s) : 0;"
                            : "res = 31 * res + (%1$s != +0.0f ? Float.floatToIntBits(%1$s) : 0);", javaName);
                        break;

                    case "double":
                        add2Fmt(hash, (tempVar ? "ig_hash_temp" : "long ig_hash_temp") +
                            " = Double.doubleToLongBits(%s);", javaName);

                        add0(hash, "");

                        add2Fmt(hash, first
                            ? "int res = (int)(ig_hash_temp ^ (ig_hash_temp >>> 32));"
                            : "res = 31 * res + (int)(ig_hash_temp ^ (ig_hash_temp >>> 32));", javaName);

                        tempVar = true;
                        break;
                }
            }
            else
                add2Fmt(hash, first ? "int res = %1$s != null ? %1$s.hashCode() : 0;"
                    : "res = 31 * res + (%1$s != null ? %1$s.hashCode() : 0);", javaName);

            first = false;
        }

        for (String line : hash)
            add0(src, line);

        add0(src, "");
        add2(src, "return res;");
        add1(src, "}");
        add0(src, "");

        // Generate toString() method.
        add1(src, "/** {@inheritDoc} */");
        add1(src, "@Override public String toString() {");

        Iterator<PojoField> it = fields.iterator();

        add2Fmt(src, "return \"%1$s [%2$s=\" + %2$s +", type, it.next().javaName());

        while (it.hasNext())
            add3(src, String.format("\", %1$s=\" + %1$s +", it.next().javaName()));

        add3(src, "\"]\";");
        add1(src, "}");

        add0(src, "}");
        add0(src, "");

        // Write generated code to file.
        try (Writer writer = new BufferedWriter(new FileWriter(out))) {
            for (String line : src)
                writer.write(line + '\n');
        }
    }

    /**
     * Generate source code for type by its metadata.
     *
     * @param pojo POJO descriptor.
     * @param outFolder Output folder.
     * @param pkg Types package.
     * @param constructor {@code true} if empty and full constructors should be generated.
     * @param includeKeys {@code true} if key fields should be included into value class.
     * @param askOverwrite Callback to ask user to confirm file overwrite.
     * @throws IOException If failed to write generated code into file.
     */
    public static void generate(PojoDescriptor pojo, String outFolder, String pkg, boolean constructor,
        boolean includeKeys, ConfirmCallable askOverwrite) throws IOException {
        File pkgFolder = new File(outFolder, pkg.replace('.', File.separatorChar));

        if (!pkgFolder.exists() && !pkgFolder.mkdirs())
            throw new IOException("Failed to create folders for package: " + pkg);

        generateCode(pojo, true, pkg, pkgFolder, constructor, false, askOverwrite);

        generateCode(pojo, false, pkg, pkgFolder, constructor, includeKeys, askOverwrite);
    }
}
