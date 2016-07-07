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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.schema.model.PojoDescriptor;
import org.apache.ignite.schema.model.PojoField;
import org.apache.ignite.schema.ui.ConfirmCallable;
import org.apache.ignite.schema.ui.MessageBox;

import static org.apache.ignite.schema.ui.MessageBox.Result.CANCEL;
import static org.apache.ignite.schema.ui.MessageBox.Result.NO;
import static org.apache.ignite.schema.ui.MessageBox.Result.NO_TO_ALL;

/**
 * Code generator of POJOs for key and value classes and configuration snippet.
 */
public class CodeGenerator {
    /** */
    private static final String TAB = "    ";
    /** */
    private static final String TAB2 = TAB + TAB;
    /** */
    private static final String TAB3 = TAB + TAB + TAB;

    /** Java key words. */
    private static final Set<String> JAVA_KEYWORDS = new HashSet<>(Arrays.asList(
        "abstract",     "assert",        "boolean",      "break",           "byte",
        "case",         "catch",         "char",         "class",           "const",
        "continue",     "default",       "do",           "double",          "else",
        "enum",         "extends",       "false",        "final",           "finally",
        "float",        "for",           "goto",         "if",              "implements",
        "import",       "instanceof",    "int",          "interface",       "long",
        "native",       "new",           "null",         "package",         "private",
        "protected",    "public",        "return",       "short",           "static",
        "strictfp",     "super",         "switch",       "synchronized",    "this",
        "throw",        "throws",        "transient",    "true",            "try",
        "void",         "volatile",      "while"
    ));

    /** java.lang.* */
    private static final String JAVA_LANG_PKG = "java.lang.";

    /** java.util.* */
    private static final String JAVA_UTIL_PKG = "java.util.";

    /** Regexp to validate java identifier. */
    private static final Pattern VALID_JAVA_IDENTIFIER =
        Pattern.compile("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*");

    /**
     * Checks if string is a valid java identifier.
     *
     * @param identifier String to check.
     * @param split If {@code true} then identifier will be split by dots.
     * @param msg Identifier type.
     * @param type Checked type.
     * @throws IllegalStateException If passed string is not valid java identifier.
     */
    private static void checkValidJavaIdentifier(String identifier, boolean split, String msg, String type)
        throws IllegalStateException {
        if (identifier.isEmpty())
            throw new IllegalStateException(msg + " could not be empty!");

        String[] parts = split ? identifier.split("\\.") : new String[] {identifier};

        if (parts.length == 0)
            throw new IllegalStateException(msg + " could not has empty parts!");

        for (String part : parts) {
            if (part.isEmpty())
                throw new IllegalStateException(msg + " could not has empty parts!");

            if (JAVA_KEYWORDS.contains(part))
                throw new IllegalStateException(msg + " could not contains reserved keyword:" +
                    " [type = " + type + ", identifier=" + identifier + ", keyword=" + part + "]");

            if (!VALID_JAVA_IDENTIFIER.matcher(part).matches())
                throw new IllegalStateException("Invalid " + msg.toLowerCase() + " name: " +
                    " [type = " + type + ", identifier=" + identifier + ", illegal part=" + part + "]");
        }
    }

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
     * @param type Full type name.
     * @return Field java type name.
     */
    private static String javaTypeName(String type) {
        return type.startsWith("java.lang.") ? type.substring(10) : type;
    }

    /**
     * @param field POJO field descriptor.
     * @return Field java type name.
     */
    private static String javaTypeName(PojoField field) {
        return javaTypeName(field.javaTypeName());
    }

    /**
     * Generate class header.
     *
     * @param src Source code.
     * @param pkg Package name.
     * @param desc Class description.
     * @param cls Class declaration.
     * @param imports Optional imports.
     */
    private static void header(Collection<String> src, String pkg, String desc, String cls, String... imports) {
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
        if (imports != null && imports.length > 0) {
            for (String imp : imports)
                add0(src, imp.isEmpty() ? "" : "import " + imp + ";");

            add0(src, "");
        }

        // Class.
        add0(src, "/**");
        add0(src, " * " + desc + " definition.");
        add0(src, " *");
        add0(src, " * Code generated by Apache Ignite Schema Import utility: "
            + new SimpleDateFormat("MM/dd/yyyy").format(new Date()) + ".");
        add0(src, " */");
        add0(src, "public class " + cls + " {");
    }

    /**
     * Write source code to file.
     *
     * @param src Source code.
     * @param out Target file.
     * @throws IOException If failed to write source code to file.
     */
    private static void write(Collection<String> src, File out) throws IOException {
        // Write generated code to file.
        try (Writer writer = new BufferedWriter(new FileWriter(out))) {
            for (String line : src)
                writer.write(line + '\n');
        }
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

        checkValidJavaIdentifier(pkg, true, "Package", type);

        checkValidJavaIdentifier(type, false, "Type", type);

        if (!pkgFolder.exists() && !pkgFolder.mkdirs())
            throw new IOException("Failed to create folders for package: " + pkg);

        File out = new File(pkgFolder, type + ".java");

        if (out.exists()) {
            MessageBox.Result choice = askOverwrite.confirm(out.getName());

            if (CANCEL == choice)
                throw new IllegalStateException("POJO generation was canceled!");

            if (NO == choice || NO_TO_ALL == choice)
                return;
        }

        Collection<String> src = new ArrayList<>(256);

        header(src, pkg, type, type + " implements Serializable", "java.io.*");

        add1(src, "/** */");
        add1(src, "private static final long serialVersionUID = 0L;");
        add0(src, "");

        Collection<PojoField> fields = key ? pojo.keyFields() : pojo.valueFields(includeKeys);

        // Generate fields declaration.
        for (PojoField field : fields) {
            String fldName = field.javaName();

            checkValidJavaIdentifier(fldName, false, "Field", type);

            add1(src, "/** Value for " + fldName + ". */");

            if (key && field.affinityKey())
                add1(src, "@AffinityKeyMapped");

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
        write(src, out);
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
    public static void pojos(PojoDescriptor pojo, String outFolder, String pkg, boolean constructor,
        boolean includeKeys, ConfirmCallable askOverwrite) throws IOException {
        File pkgFolder = new File(outFolder, pkg.replace('.', File.separatorChar));

        generateCode(pojo, true, pkg, pkgFolder, constructor, false, askOverwrite);

        generateCode(pojo, false, pkg, pkgFolder, constructor, includeKeys, askOverwrite);
    }

    /**
     * Add type fields.
     *
     * @param src Source code lines.
     * @param owner Fields owner collection.
     * @param fields Fields metadata.
     */
    private static void addFields(Collection<String> src, String owner, Collection<PojoField> fields) {
        for (PojoField field : fields) {
            String javaTypeName = field.javaTypeName();

            if (javaTypeName.startsWith(JAVA_LANG_PKG))
                javaTypeName = javaTypeName.substring(JAVA_LANG_PKG.length());
            else if (javaTypeName.startsWith(JAVA_UTIL_PKG))
                javaTypeName = javaTypeName.substring(JAVA_UTIL_PKG.length());

            add2(src, owner + ".add(new JdbcTypeField(Types." + field.dbTypeName() + ", \"" + field.dbName() + "\", " +
                javaTypeName + ".class, \"" + field.javaName() + "\"));");
        }
    }

    /**
     * Generate java snippet for cache configuration with JDBC store and types metadata.
     *
     * @param pojos POJO descriptors.
     * @param pkg Types package.
     * @param includeKeys {@code true} if key fields should be included into value class.
     * @param generateAliases {@code true} if aliases should be generated for query fields.
     * @param outFolder Output folder.
     * @param askOverwrite Callback to ask user to confirm file overwrite.
     * @throws IOException If generation failed.
     */
    public static void snippet(Collection<PojoDescriptor> pojos, String pkg, boolean includeKeys,
        boolean generateAliases, String outFolder, ConfirmCallable askOverwrite) throws IOException {
        File pkgFolder = new File(outFolder, pkg.replace('.', File.separatorChar));

        File cacheCfg = new File(pkgFolder, "CacheConfig.java");

        if (cacheCfg.exists()) {
            MessageBox.Result choice = askOverwrite.confirm(cacheCfg.getName());

            if (CANCEL == choice)
                throw new IllegalStateException("Java snippet generation was canceled!");

            if (NO == choice || NO_TO_ALL == choice)
                return;
        }

        Collection<String> src = new ArrayList<>(256);

        header(src, pkg, "CacheConfig", "CacheConfig", "java.sql.*", "java.util.*", "", "org.apache.ignite.cache.*",
            "org.apache.ignite.cache.store.jdbc.*", "org.apache.ignite.configuration.*");

        // Generate methods for each type in order to avoid compiler error "java: code too large".
        for (PojoDescriptor pojo : pojos) {
            String tbl = pojo.table();
            String valClsName = pojo.valueClassName();
            Collection<PojoField> fields = pojo.valueFields(true);

            add1(src, "/**");
            add1(src, " * Create JDBC type for " + tbl + ".");
            add1(src, " *");
            add1(src, " * @param cacheName Cache name.");
            add1(src, " * @return Configured JDBC type.");
            add1(src, " */");
            add1(src, "private static JdbcType jdbcType" + valClsName + "(String cacheName) {");

            add2(src, "JdbcType jdbcType = new JdbcType();");
            add0(src, "");

            add2(src, "jdbcType.setCacheName(cacheName);");

            // Database schema.
            if (pojo.schema() != null)
                add2(src, "jdbcType.setDatabaseSchema(\"" + pojo.schema() + "\");");

            // Database table.
            add2(src, "jdbcType.setDatabaseTable(\"" + tbl + "\");");

            // Java info.
            add2(src, "jdbcType.setKeyType(\"" + pkg + "." + pojo.keyClassName() + "\");");
            add2(src, "jdbcType.setValueType(\"" + pkg + "." + valClsName + "\");");
            add0(src, "");

            // Key fields.
            add2(src, "// Key fields for " + tbl + ".");
            add2(src, "Collection<JdbcTypeField> keys = new ArrayList<>();");
            addFields(src, "keys", pojo.keyFields());
            add2(src, "jdbcType.setKeyFields(keys.toArray(new JdbcTypeField[keys.size()]));");
            add0(src, "");

            // Value fields.
            add2(src, "// Value fields for " + tbl + ".");
            add2(src, "Collection<JdbcTypeField> vals = new ArrayList<>();");
            addFields(src, "vals", pojo.valueFields(includeKeys));
            add2(src, "jdbcType.setValueFields(vals.toArray(new JdbcTypeField[vals.size()]));");
            add0(src, "");
            add2(src, "return jdbcType;");
            add1(src, "}");
            add0(src, "");

            add1(src, "/**");
            add1(src, " * Create SQL Query descriptor for " + tbl + ".");
            add1(src, " *");
            add1(src, " * @return Configured query entity.");
            add1(src, " */");
            add1(src, "private static QueryEntity queryEntity" + valClsName + "() {");

            // Query entity.
            add2(src, "QueryEntity qryEntity = new QueryEntity();");
            add0(src, "");
            add2(src, "qryEntity.setKeyType(\"" + pkg + "." + pojo.keyClassName() + "\");");
            add2(src, "qryEntity.setValueType(\"" + pkg + "." + valClsName + "\");");

            add0(src, "");

            // Query fields.
            add2(src, "// Query fields for " + tbl + ".");
            add2(src, "LinkedHashMap<String, String> fields = new LinkedHashMap<>();");
            add0(src, "");

            for (PojoField field : fields)
                add2(src, "fields.put(\"" + field.javaName() + "\", \"" +
                    GeneratorUtils.boxPrimitiveType(field.javaTypeName()) + "\");");

            add0(src, "");
            add2(src, "qryEntity.setFields(fields);");
            add0(src, "");

            // Aliases.
            if (generateAliases) {
                Collection<PojoField> aliases = new ArrayList<>();

                for (PojoField field : fields) {
                    if (!field.javaName().equalsIgnoreCase(field.dbName()))
                        aliases.add(field);
                }

                if (!aliases.isEmpty()) {
                    add2(src, "// Aliases for fields.");
                    add2(src, "Map<String, String> aliases = new HashMap<>();");
                    add0(src, "");

                    for (PojoField alias : aliases)
                        add2(src, "aliases.put(\"" + alias.javaName() + "\", \"" + alias.dbName() + "\");");

                    add0(src, "");
                    add2(src, "qryEntity.setAliases(aliases);");
                    add0(src, "");
                }
            }

            // Indexes.
            Collection<QueryIndex> idxs = pojo.indexes();

            if (!idxs.isEmpty()) {
                boolean first = true;
                boolean firstIdx = true;

                for (QueryIndex idx : idxs) {
                    Set<Map.Entry<String, Boolean>> dbIdxFlds = idx.getFields().entrySet();

                    int sz = dbIdxFlds.size();

                    List<T2<String, Boolean>> idxFlds = new ArrayList<>(sz);

                    for (Map.Entry<String, Boolean> idxFld : dbIdxFlds) {
                        PojoField field = GeneratorUtils.findFieldByName(fields, idxFld.getKey());

                        if (field != null)
                            idxFlds.add(new T2<>(field.javaName(), idxFld.getValue()));
                        else
                            break;
                    }

                    // Only if all fields present, add index description.
                    if (idxFlds.size() == sz) {
                        if (first) {
                            add2(src, "// Indexes for " + tbl + ".");
                            add2(src, "Collection<QueryIndex> idxs = new ArrayList<>();");
                            add0(src, "");
                        }

                        if (sz == 1) {
                            T2<String, Boolean> idxFld = idxFlds.get(0);

                            add2(src, "idxs.add(new QueryIndex(\"" + idxFld.getKey() + "\", " + idxFld.getValue() + ", \"" +
                                idx.getName() + "\"));");
                            add0(src, "");
                        }
                        else {
                            add2(src, (firstIdx ? "QueryIndex " : "") + "idx = new QueryIndex();");
                            add0(src, "");

                            add2(src, "idx.setName(\"" + idx.getName() + "\");");
                            add0(src, "");

                            add2(src, (firstIdx ? "LinkedHashMap<String, Boolean> " : "") +
                                "idxFlds = new LinkedHashMap<>();");
                            add0(src, "");

                            for (T2<String, Boolean> idxFld : idxFlds)
                                add2(src, "idxFlds.put(\"" + idxFld.getKey() + "\", " + idxFld.getValue() + ");");

                            add0(src, "");

                            add2(src, "idx.setFields(idxFlds);");
                            add0(src, "");

                            add2(src, "idxs.add(idx);");
                            add0(src, "");

                            firstIdx = false;
                        }

                        first = false;
                    }
                }

                if (!first) {
                    add2(src, "qryEntity.setIndexes(idxs);");
                    add0(src, "");
                }
            }

            add2(src, "return qryEntity;");

            add1(src, "}");
            add0(src, "");
        }

        add1(src, "/**");
        add1(src, " * Configure cache.");
        add1(src, " *");
        add1(src, " * @param cacheName Cache name.");
        add1(src, " * @param storeFactory Cache store factory.");
        add1(src, " * @return Cache configuration.");
        add1(src, " */");
        add1(src, "public static <K, V> CacheConfiguration<K, V> cache(String cacheName," +
            " CacheJdbcPojoStoreFactory<K, V> storeFactory) {");
        add2(src, "if (storeFactory == null)");
        add3(src, " throw new IllegalArgumentException(\"Cache store factory cannot be null.\");");
        add0(src, "");
        add2(src, "CacheConfiguration<K, V> ccfg = new CacheConfiguration<>(cacheName);");
        add0(src, "");
        add2(src, "ccfg.setCacheStoreFactory(storeFactory);");
        add2(src, "ccfg.setReadThrough(true);");
        add2(src, "ccfg.setWriteThrough(true);");
        add0(src, "");

        add2(src, "// Configure JDBC types. ");
        add2(src, "Collection<JdbcType> jdbcTypes = new ArrayList<>();");
        add0(src, "");

        for (PojoDescriptor pojo : pojos)
            add2(src, "jdbcTypes.add(jdbcType" + pojo.valueClassName() + "(cacheName));");

        add0(src, "");

        add2(src, "storeFactory.setTypes(jdbcTypes.toArray(new JdbcType[jdbcTypes.size()]));");
        add0(src, "");


        add2(src, "// Configure query entities. ");
        add2(src, "Collection<QueryEntity> qryEntities = new ArrayList<>();");
        add0(src, "");

        for (PojoDescriptor pojo : pojos)
            add2(src, "qryEntities.add(queryEntity" + pojo.valueClassName() + "());");

        add0(src, "");

        add2(src, "ccfg.setQueryEntities(qryEntities);");
        add0(src, "");

        add2(src, "return ccfg;");
        add1(src, "}");

        add0(src, "}");

        write(src, cacheCfg);
    }
}
