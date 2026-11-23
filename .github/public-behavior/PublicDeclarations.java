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

import java.io.IOException;
import java.io.StringReader;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.ignite.SystemProperty;
import org.apache.ignite.internal.commandline.ArgumentParser;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgument;
import org.apache.ignite.internal.management.IgniteCommandRegistry;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable;
import org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteStdSqlOperatorTable;
import org.apache.ignite.internal.processors.query.calcite.sql.generated.IgniteSqlParserImpl;
import org.apache.ignite.internal.systemview.SystemViewRowAttributeWalkerGenerator;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.apache.ignite.startup.cmdline.CommandLineStartup;

/**
 * Prints the public declarations of a built Ignite source tree, one per line: area, key, value, flags, source location and
 * rename group, separated by tabs. Flags mark deprecated and experimental declarations. A rename group is an internal Java
 * name that the key contains: renaming it changes nothing for users. Descriptions, help texts and examples are left out,
 * so editing them changes nothing either.
 * <p>
 * Usage: {@code java -cp <classpath of the built tree> PublicDeclarations.java <tree root>}.
 */
@SuppressWarnings("unchecked")
public class PublicDeclarations {
    /** */
    private static final String DEPRECATED = "deprecated";

    /** */
    private static final String EXPERIMENTAL = "experimental";

    /** Words that start a statement rather than a declaration. */
    private static final Set<String> STATEMENTS = Set.of("return", "new", "throw", "case", "else", "assert", "yield");

    /** Internal classes that declare protocol, storage and error constants: area, class, names of the constants. */
    private static final String[][] CONSTANTS = {
        {"Thin client protocol", "org.apache.ignite.internal.processors.platform.client.ClientMessageParser", "OP_.*"},
        {"Thin client protocol", "org.apache.ignite.internal.processors.platform.client.ClientStatus", ".*"},
        {"Thin client protocol", "org.apache.ignite.internal.processors.platform.client.ClientConnectionContext", "VER_.*|DEFAULT_VER"},
        {"JDBC protocol", "org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest", "[A-Z_]+"},
        {"JDBC protocol", "org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext", "VER_.*|CURRENT_VER"},
        {"ODBC protocol", "org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest", "[A-Z_]+"},
        {"ODBC protocol", "org.apache.ignite.internal.processors.odbc.odbc.OdbcConnectionContext", "VER_.*|CURRENT_VER"},
        {"Binary format", "org.apache.ignite.internal.binary.GridBinaryMarshaller", "[A-Z_0-9]+"},
        {"Stored data", "org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO", "T_.*"},
        {"Stored data", "org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata", "serialVersionUID"},
        {"Errors", "org.apache.ignite.internal.processors.odbc.SqlStateCode", "[A-Z_0-9]+"},
        {"Errors", "org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode", "[A-Z_0-9]+"},
        {"CLI", "org.apache.ignite.internal.commandline.CommandHandler", "EXIT_CODE_.*|ERR_.*"},
        {"SQL keywords", "org.apache.ignite.internal.sql.SqlKeyword", "[A-Z_0-9]+"},
    };

    /** Internal enums whose constants users send or store by a code: area, enum, method that returns the code. */
    private static final String[][] CODED_ENUMS = {
        {"Thin client protocol", "org.apache.ignite.internal.processors.platform.client.ClientBitmaskFeature", "featureId"},
        {"Thin client protocol", "org.apache.ignite.internal.client.thin.ProtocolBitmaskFeature", "featureId"},
        {"REST", "org.apache.ignite.internal.processors.rest.GridRestCommand", "key"},
        {"Stored data", "org.apache.ignite.internal.pagemem.wal.record.WALRecord$RecordType", "index"},
    };

    /** Declarations by area and key. */
    private static final Map<String, Declaration> decls = new TreeMap<>();

    /** Problems that make the description incomplete. */
    private static final List<String> errors = new ArrayList<>();

    /** Package patterns that the javadoc build excludes. */
    private static final List<String> excludedPkgs = new ArrayList<>();

    /** Source files of published top-level classes. */
    private static final Map<String, Path> sources = new HashMap<>();

    /** Lines of source files read so far. */
    private static final Map<Path, List<String>> texts = new HashMap<>();

    /** Tree root. */
    private static Path root;

    /** @param args Tree root. */
    public static void main(String[] args) throws Exception {
        root = Path.of(args[0]).toAbsolutePath().normalize();

        List<Path> modules = publishedModules();
        List<Path> classDirs = modules.stream().map(m -> m.resolve("target/classes")).filter(Files::isDirectory)
            .collect(Collectors.toList());

        javadocExclusions();
        indexSources(modules);

        javaApi(classDirs);
        systemProperties();
        systemViews(classDirs);
        cli();
        constants();
        sql();
        distribution();

        for (Declaration d : decls.values()) {
            System.out.println(String.join("\t", d.area, d.key, String.join(" | ", d.values), String.join(",", d.flags), d.location,
                d.group));
        }

        if (!errors.isEmpty()) {
            errors.forEach(System.err::println);

            System.exit(1);
        }
    }

    /** Adds a declaration whose key has no internal names. */
    private static void add(String area, String key, String val, String flags, String location) {
        add(area, key, val, flags, location, "");
    }

    /**
     * Adds a declaration. Declarations with the same key, such as a system property that two constants declare, are merged:
     * their values are listed together, so a change to any of them is a change of the merged declaration.
     */
    private static void add(String area, String key, String val, String flags, String location, String group) {
        Declaration d = decls.computeIfAbsent(area + '\t' + oneLine(key), k -> new Declaration(area, oneLine(key), location, group));

        d.values.add(oneLine(val));

        if (!flags.isEmpty())
            d.flags.addAll(Arrays.asList(flags.split(",")));
    }

    /** Declaration as it is printed. */
    private static class Declaration {
        /** */
        final String area;

        /** */
        final String key;

        /** */
        final String location;

        /** */
        final String group;

        /** */
        final Set<String> values = new TreeSet<>();

        /** */
        final Set<String> flags = new TreeSet<>();

        /** */
        Declaration(String area, String key, String location, String group) {
            this.area = area;
            this.key = key;
            this.location = location;
            this.group = group;
        }
    }

    /** */
    private static String oneLine(String s) {
        return s.replaceAll("\\s+", " ").trim();
    }

    /** @return Flags of a declaration. */
    private static String flags(boolean deprecated, boolean experimental) {
        return deprecated && experimental ? DEPRECATED + ',' + EXPERIMENTAL : deprecated ? DEPRECATED : experimental ? EXPERIMENTAL : "";
    }

    /**
     * Describes the artifacts that {@code ignite-bom} declares.
     *
     * @return Directories of the published modules built from this tree.
     */
    private static List<Path> publishedModules() throws IOException {
        Map<String, Path> modules = new HashMap<>();

        try (Stream<Path> poms = Files.walk(root.resolve("modules"), 3)) {
            for (Path pom : (Iterable<Path>)poms.filter(p -> p.endsWith("pom.xml"))::iterator)
                modules.put(tag(Files.readString(pom).replaceAll("(?s)<parent>.*?</parent>", ""), "artifactId"), pom.getParent());
        }

        List<Path> res = new ArrayList<>();
        String bom = "modules/bom/pom.xml";
        Matcher dep = Pattern.compile("(?s)<dependency>(.*?)</dependency>").matcher(Files.readString(root.resolve(bom)));

        while (dep.find()) {
            String artifact = tag(dep.group(1), "artifactId");
            String type = tag(dep.group(1), "type");

            add("Distribution", "ignite-bom: org.apache.ignite:" + artifact + (type == null ? "" : ':' + type), "", "", bom);

            if (type == null && modules.containsKey(artifact))
                res.add(modules.get(artifact));
        }

        return res;
    }

    /** @return Content of the first tag with the given name, or {@code null}. */
    private static String tag(String xml, String name) {
        Matcher m = Pattern.compile("<" + name + ">\\s*(.*?)\\s*</" + name + ">", Pattern.DOTALL).matcher(xml);

        return m.find() ? m.group(1) : null;
    }

    /** Reads the packages that the javadoc build leaves out of the public API. */
    private static void javadocExclusions() throws IOException {
        String names = tag(Files.readString(root.resolve("pom.xml")), "excludePackageNames");

        for (String p : names.replaceAll("(?s)<!--.*?-->", "").split("[:\\s]+")) {
            if (!p.isEmpty())
                excludedPkgs.add(p);
        }
    }

    /** @return {@code True} if the javadoc build publishes the package. */
    private static boolean publicPackage(String pkg) {
        for (String p : excludedPkgs) {
            if (p.endsWith(".*") ? pkg.startsWith(p.substring(0, p.length() - 1)) : pkg.equals(p))
                return false;
        }

        return true;
    }

    /** Maps top-level classes of the published modules to their source files. */
    private static void indexSources(List<Path> modules) throws IOException {
        for (Path module : modules) {
            Path src = module.resolve("src/main/java");

            if (!Files.isDirectory(src))
                continue;

            try (Stream<Path> files = Files.walk(src)) {
                for (Path f : (Iterable<Path>)files.filter(p -> p.toString().endsWith(".java"))::iterator) {
                    String rel = src.relativize(f).toString();

                    sources.put(rel.substring(0, rel.length() - ".java".length()).replace('/', '.'), f);
                }
            }
        }
    }

    /**
     * @param cls Class.
     * @param member Member name, or {@code null} for the class itself.
     * @param params Number of parameters of a method or constructor, or -1 for a field or the class.
     * @param deprecated Whether to point at the {@code @Deprecated} annotation of the declaration.
     * @return Source file and line of the declaration, or the file alone if the line is not certain, or an empty string if
     *      the class has no source file. For a deprecated declaration the line is that of its {@code @Deprecated}.
     */
    private static String where(Class<?> cls, String member, int params, boolean deprecated) {
        Class<?> top = cls;

        while (top.getDeclaringClass() != null)
            top = top.getDeclaringClass();

        Path src = sources.get(top.getName());

        if (src == null)
            return "";

        List<String> text = texts.computeIfAbsent(src, f -> {
            try {
                return Files.readAllLines(f);
            }
            catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });

        String file = root.relativize(src).toString();
        Pattern typeDecl = Pattern.compile("\\b(class|interface|enum|@interface)\\s+" + Pattern.quote(cls.getSimpleName()) + "\\b");
        int type = -1;

        for (int i = 0; i < text.size() && type < 0; i++)
            type = typeDecl.matcher(text.get(i)).find() ? i : -1;

        int line = type;

        if (member != null) {
            List<Integer> decls = declarations(text, Math.max(type, 0), member, params);

            if (deprecated && decls.size() > 1)
                decls = decls.stream().filter(i -> deprecatedAt(text, i) >= 0).collect(Collectors.toList());

            if (decls.size() != 1)
                return deprecated || type < 0 ? file : file + ':' + (type + 1);

            line = decls.get(0);
        }

        if (line < 0)
            return file;

        if (deprecated) {
            int d = deprecatedAt(text, line);

            return d < 0 ? file : file + ':' + (d + 1);
        }

        return file + ':' + (line + 1);
    }

    /** @return Lines from {@code from} that declare the member, not just use it. */
    private static List<Integer> declarations(List<String> text, int from, String name, int params) {
        Pattern decl = Pattern.compile("^\\s*(?:([^=(;]*[\\w>\\]])\\s+)?" + Pattern.quote(name) + "\\s*([(=;,{]|$)");
        List<Integer> res = new ArrayList<>();

        for (int i = from; i < text.size(); i++) {
            Matcher m = decl.matcher(text.get(i));

            if (!m.find())
                continue;

            String prefix = m.group(1);
            String[] words = prefix == null ? new String[0] : prefix.trim().split("\\s+");

            if (prefix != null && STATEMENTS.contains(words[words.length - 1]))
                continue;

            boolean declares = params < 0
                ? prefix != null || !"=".equals(m.group(2))
                : "(".equals(m.group(2)) && prefix != null && paramCount(text, i, m.end()) == params;

            if (declares)
                res.add(i);
        }

        return res;
    }

    /** @return Number of parameters in the list that starts right before {@code start} in the given line. */
    private static int paramCount(List<String> text, int line, int start) {
        StringBuilder list = new StringBuilder();
        int depth = 1;

        for (int i = line; i < text.size() && i < line + 10 && depth > 0; i++) {
            String s = i == line ? text.get(i).substring(start) : text.get(i);

            for (char c : s.toCharArray()) {
                depth += c == '(' || c == '<' ? 1 : c == ')' || c == '>' ? -1 : 0;

                if (depth == 0)
                    break;

                list.append(depth == 1 && c == ',' ? '\u0000' : c);
            }
        }

        String params = list.toString().trim();

        return params.isEmpty() ? 0 : params.split("\u0000", -1).length;
    }

    /** @return Line of the {@code @Deprecated} annotation of the declaration at {@code line}, or -1. */
    private static int deprecatedAt(List<String> text, int line) {
        for (int i = line; i >= 0 && line - i < 20; i--) {
            if (text.get(i).contains("@Deprecated"))
                return i;

            String l = text.get(i).trim();

            if (i < line && (l.endsWith(";") || l.endsWith("}") || l.endsWith("{") || l.endsWith("*/")))
                return -1;
        }

        return -1;
    }

    /** @return Classes in the package of the class directories, loaded without initialization. */
    private static List<Class<?>> classes(List<Path> classDirs, String pkg, boolean publicOnly) throws IOException {
        List<Class<?>> res = new ArrayList<>();

        for (Path dir : classDirs) {
            Path pkgDir = dir.resolve(pkg.replace('.', '/'));

            if (!Files.isDirectory(pkgDir))
                continue;

            try (Stream<Path> files = pkg.isEmpty() ? Files.walk(pkgDir) : Files.list(pkgDir)) {
                for (Path f : (Iterable<Path>)files.filter(p -> p.toString().endsWith(".class"))::iterator) {
                    String rel = dir.relativize(f).toString();
                    String name = rel.substring(0, rel.length() - ".class".length()).replace('/', '.');

                    if (name.endsWith("module-info") || name.endsWith("package-info")
                        || publicOnly && !publicPackage(name.substring(0, Math.max(0, name.lastIndexOf('.')))))
                        continue;

                    try {
                        res.add(Class.forName(name, false, PublicDeclarations.class.getClassLoader()));
                    }
                    catch (Throwable e) {
                        errors.add("Failed to load " + name + ": " + e);
                    }
                }
            }
        }

        return res;
    }

    /**
     * Describes the public and protected declarations of the packages published in javadoc. Types generated during the build
     * have no source file and are skipped: they serialize node-to-node messages, which are outside the public contract.
     */
    private static void javaApi(List<Path> classDirs) throws IOException {
        int types = 0;

        for (Class<?> cls : classes(classDirs, "", true)) {
            try {
                if (apiType(cls) && !where(cls, null, -1, false).isEmpty()) {
                    javaType(cls);

                    types++;
                }
            }
            catch (Throwable e) {
                errors.add("Failed to describe " + cls.getName() + ": " + e);
            }
        }

        if (types == 0)
            errors.add("No public types found in " + classDirs);
    }

    /** @return {@code True} if users can reference the type. */
    private static boolean apiType(Class<?> cls) {
        if (cls.isSynthetic() || cls.isAnonymousClass() || cls.isLocalClass() || !publicPackage(cls.getPackageName()))
            return false;

        Class<?> owner = cls.getDeclaringClass();

        return owner == null ? Modifier.isPublic(cls.getModifiers()) : accessible(cls.getModifiers(), owner) && apiType(owner);
    }

    /** @return {@code True} if users can reach a member with these modifiers. */
    private static boolean accessible(int mod, Class<?> owner) {
        return Modifier.isPublic(mod) || Modifier.isProtected(mod) && !Modifier.isFinal(owner.getModifiers()) && !owner.isEnum();
    }

    /** @return {@code True} if the type belongs to Ignite but not to its public API. */
    private static boolean internalType(Class<?> cls) {
        return (cls.getName().startsWith("org.apache.ignite.") || cls.getName().startsWith("org.jsr166.")) && !apiType(cls);
    }

    /** @return {@code True} if the element or a type that encloses it has the annotation. */
    private static boolean marked(AnnotatedElement e, Class<?> owner, Class<? extends java.lang.annotation.Annotation> ann) {
        if (e.isAnnotationPresent(ann))
            return true;

        for (Class<?> c = owner; c != null; c = c.getDeclaringClass()) {
            if (c.isAnnotationPresent(ann) || c.getPackage() != null && c.getPackage().isAnnotationPresent(ann))
                return true;
        }

        return false;
    }

    /**
     * @return Names of the type parameters of the class and of the classes that enclose it, by position: renaming a type
     *      parameter does not change what users can write.
     */
    private static Map<TypeVariable<?>, String> typeVars(Class<?> cls) {
        Map<TypeVariable<?>, String> res = new HashMap<>();
        int depth = 0;

        for (Class<?> c = cls; c != null; c = c.getDeclaringClass(), depth++) {
            TypeVariable<?>[] vars = c.getTypeParameters();

            for (int i = 0; i < vars.length; i++)
                res.put(vars[i], "#" + (depth == 0 ? "" : depth + ".") + (i + 1));
        }

        return res;
    }

    /** @return Type parameters of a method or constructor named by position, added to those of its class. */
    private static Map<TypeVariable<?>, String> typeVars(Executable e, Map<TypeVariable<?>, String> classVars) {
        Map<TypeVariable<?>, String> res = new HashMap<>(classVars);
        TypeVariable<?>[] vars = e.getTypeParameters();

        for (int i = 0; i < vars.length; i++)
            res.put(vars[i], "#m" + (i + 1));

        return res;
    }

    /** @return Type as users write it, with type parameters named by position. */
    private static String typeText(Type t, Map<TypeVariable<?>, String> vars) {
        if (t instanceof Class)
            return ((Class<?>)t).isArray() ? typeText(((Class<?>)t).getComponentType(), vars) + "[]" : t.getTypeName();

        if (t instanceof ParameterizedType) {
            ParameterizedType p = (ParameterizedType)t;

            return typeText(p.getRawType(), vars)
                + Arrays.stream(p.getActualTypeArguments()).map(a -> typeText(a, vars)).collect(Collectors.joining(", ", "<", ">"));
        }

        if (t instanceof TypeVariable)
            return vars.getOrDefault(t, ((TypeVariable<?>)t).getName());

        if (t instanceof WildcardType) {
            WildcardType w = (WildcardType)t;

            if (w.getLowerBounds().length > 0)
                return "? super " + typeText(w.getLowerBounds()[0], vars);

            return w.getUpperBounds()[0] == Object.class ? "?" : "? extends " + typeText(w.getUpperBounds()[0], vars);
        }

        if (t instanceof GenericArrayType)
            return typeText(((GenericArrayType)t).getGenericComponentType(), vars) + "[]";

        return t.getTypeName();
    }

    /** Describes a public type and the members that users can call or extend. */
    private static void javaType(Class<?> cls) {
        String name = cls.getCanonicalName();
        boolean deprecated = marked(cls, cls.getDeclaringClass(), Deprecated.class);
        boolean experimental = marked(cls, cls.getDeclaringClass(), IgniteExperimental.class);
        Map<TypeVariable<?>, String> vars = typeVars(cls);

        Set<String> supers = new TreeSet<>();

        collectSupertypes(cls, vars, supers);

        String kind = cls.isAnnotation() ? "@interface" : cls.isInterface() ? "interface" : cls.isEnum() ? "enum" : "class";
        int mask = Modifier.PUBLIC | Modifier.PROTECTED | Modifier.STATIC
            | (cls.isInterface() || cls.isEnum() ? 0 : Modifier.FINAL | Modifier.ABSTRACT);
        String typeWhere = where(cls, null, -1, deprecated);

        add("Java API", name, markers(cls) + modifiers(cls.getModifiers() & mask) + kind + ' ' + cls.getSimpleName()
            + typeParams(cls.getTypeParameters(), vars) + (supers.isEmpty() ? "" : " : " + String.join(", ", supers))
            + annotationUse(cls), flags(deprecated, experimental), typeWhere);

        Map<String, Member> members = new HashMap<>();

        describeMembers(cls, cls, vars, members);

        Set<Class<?>> internalSupers = new LinkedHashSet<>();

        collectInternalSupertypes(cls, internalSupers);

        for (Class<?> sup : internalSupers)
            describeMembers(sup, cls, vars, members);

        members.forEach((key, m) -> {
            boolean own = m.elem.isAnnotationPresent(Deprecated.class);
            boolean exp = experimental || marked(m.elem, m.owner, IgniteExperimental.class);
            String location = own ? where(m.owner, m.name, m.params, true) : deprecated ? typeWhere : where(m.owner, m.name, m.params, false);

            add("Java API", name + '#' + key, m.text, flags(deprecated || own, exp), location);
        });
    }

    /** @return Where an annotation type may be used and how long it is kept, or an empty string for other types. */
    private static String annotationUse(Class<?> cls) {
        if (!cls.isAnnotation())
            return "";

        Target target = cls.getAnnotation(Target.class);
        Retention retention = cls.getAnnotation(Retention.class);

        return " @Target" + (target == null ? "(default)"
            : Arrays.stream(target.value()).map(Enum::name).sorted().collect(Collectors.joining(", ", "{", "}")))
            + " @Retention(" + (retention == null ? "CLASS" : retention.value()) + ')'
            + (cls.isAnnotationPresent(Inherited.class) ? " @Inherited" : "")
            + (cls.isAnnotationPresent(Repeatable.class) ? " @Repeatable" : "");
    }

    /** Member of a public type. */
    private static class Member {
        /** */
        final AnnotatedElement elem;

        /** Class that declares the member. */
        final Class<?> owner;

        /** Name in the source. */
        final String name;

        /** Number of parameters of a method or constructor, or -1 for a field. */
        final int params;

        /** Description. */
        final String text;

        /** */
        Member(AnnotatedElement elem, Class<?> owner, String name, int params, String text) {
            this.elem = elem;
            this.owner = owner;
            this.name = name;
            this.params = params;
            this.text = text;
        }
    }

    /** Collects public supertypes, looking through Ignite internal ones. */
    private static void collectSupertypes(Class<?> cls, Map<TypeVariable<?>, String> vars, Set<String> res) {
        List<Type> direct = new ArrayList<>(Arrays.asList(cls.getGenericInterfaces()));

        if (cls.getGenericSuperclass() != null && cls.getSuperclass() != Object.class && !cls.isEnum())
            direct.add(cls.getGenericSuperclass());

        for (Type t : direct) {
            Class<?> raw = t instanceof ParameterizedType ? (Class<?>)((ParameterizedType)t).getRawType() : (Class<?>)t;

            if (internalType(raw))
                collectSupertypes(raw, vars, res);
            else if (raw != java.lang.annotation.Annotation.class)
                res.add(typeText(t, vars));
        }
    }

    /** Collects Ignite internal supertypes whose members users reach through the public type. */
    private static void collectInternalSupertypes(Class<?> cls, Set<Class<?>> res) {
        List<Class<?>> direct = new ArrayList<>(Arrays.asList(cls.getInterfaces()));

        if (cls.getSuperclass() != null)
            direct.add(cls.getSuperclass());

        for (Class<?> sup : direct) {
            if (internalType(sup) && res.add(sup))
                collectInternalSupertypes(sup, res);
        }
    }

    /** Describes the members of {@code src} as members of the public type {@code api}, keeping members described earlier. */
    private static void describeMembers(Class<?> src, Class<?> api, Map<TypeVariable<?>, String> vars, Map<String, Member> res) {
        for (Constructor<?> c : src == api ? src.getDeclaredConstructors() : new Constructor<?>[0]) {
            if (!c.isSynthetic() && accessible(c.getModifiers(), api)) {
                Map<TypeVariable<?>, String> v = typeVars(c, vars);

                res.putIfAbsent("<init>" + params(c), new Member(c, src, api.getSimpleName(), c.getParameterCount(),
                    markers(c) + modifiers(c.getModifiers()) + typeParams(c.getTypeParameters(), v) + api.getSimpleName() + signature(c, v)));
            }
        }

        for (Method m : src.getDeclaredMethods()) {
            if (m.isSynthetic() || m.isBridge() || !accessible(m.getModifiers(), api))
                continue;

            Map<TypeVariable<?>, String> v = typeVars(m, vars);
            String dflt = m.getDefaultValue() == null ? "" : " default " + valueText(m.getDefaultValue());

            res.putIfAbsent(m.getName() + params(m), new Member(m, src, m.getName(), m.getParameterCount(), markers(m)
                + modifiers(m.getModifiers()) + (m.isDefault() ? "default " : "") + typeParams(m.getTypeParameters(), v)
                + typeText(m.getGenericReturnType(), v) + ' ' + m.getName() + signature(m, v) + dflt));
        }

        for (Field f : src.getDeclaredFields()) {
            if (f.isSynthetic() || !accessible(f.getModifiers(), api))
                continue;

            res.putIfAbsent(f.getName(), new Member(f, src, f.getName(), -1, markers(f) + modifiers(f.getModifiers())
                + typeText(f.getGenericType(), vars) + ' ' + f.getName() + constantValue(f)));
        }
    }

    /** @return Erased parameter types that identify an overload. */
    private static String params(Executable e) {
        return Arrays.stream(e.getParameterTypes()).map(Class::getTypeName).collect(Collectors.joining(",", "(", ")"));
    }

    /** @return Generic parameter and exception types. */
    private static String signature(Executable e, Map<TypeVariable<?>, String> vars) {
        Type[] params = e.getGenericParameterTypes();
        List<String> names = new ArrayList<>();

        for (int i = 0; i < params.length; i++) {
            String n = typeText(params[i], vars);

            names.add(e.isVarArgs() && i == params.length - 1 ? n.substring(0, n.length() - 2) + "..." : n);
        }

        Type[] exceptions = e.getGenericExceptionTypes();

        return '(' + String.join(", ", names) + ')' + (exceptions.length == 0 ? "" :
            Arrays.stream(exceptions).map(t -> typeText(t, vars)).sorted().collect(Collectors.joining(", ", " throws ", "")));
    }

    /** @return Type parameters with their bounds. */
    private static String typeParams(TypeVariable<?>[] params, Map<TypeVariable<?>, String> vars) {
        if (params.length == 0)
            return "";

        return Arrays.stream(params).map(p -> {
            List<String> bounds = Arrays.stream(p.getBounds()).filter(b -> b != Object.class).map(b -> typeText(b, vars))
                .collect(Collectors.toList());

            return vars.get(p) + (bounds.isEmpty() ? "" : " extends " + String.join(" & ", bounds));
        }).collect(Collectors.joining(", ", "<", "> "));
    }

    /** @return Annotations that change what users may rely on. */
    private static String markers(AnnotatedElement e) {
        return (e.isAnnotationPresent(Deprecated.class) ? "@Deprecated " : "")
            + (e.isAnnotationPresent(IgniteExperimental.class) ? "@IgniteExperimental " : "");
    }

    /** */
    private static String modifiers(int mod) {
        String s = Modifier.toString(mod & (Modifier.PUBLIC | Modifier.PROTECTED | Modifier.STATIC | Modifier.FINAL | Modifier.ABSTRACT));

        return s.isEmpty() ? "" : s + ' ';
    }

    /** @return Enum ordinal or the value of a constant that compilers copy into user code. */
    private static String constantValue(Field f) {
        if (f.isEnumConstant())
            return " #" + ((Enum<?>)staticValue(f)).ordinal();

        int mod = f.getModifiers();

        if (!Modifier.isStatic(mod) || !Modifier.isFinal(mod) || !(f.getType().isPrimitive() || f.getType() == String.class))
            return "";

        return " = " + valueText(staticValue(f));
    }

    /** */
    private static Object staticValue(Field f) {
        try {
            f.setAccessible(true);

            return f.get(null);
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /** */
    private static String valueText(Object val) {
        if (val instanceof String)
            return '"' + ((String)val).replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\t", "\\t") + '"';

        if (val instanceof Character)
            return "'" + val + "'";

        if (val instanceof Class)
            return ((Class<?>)val).getTypeName() + ".class";

        if (val instanceof Enum)
            return ((Enum<?>)val).getDeclaringClass().getTypeName() + '.' + ((Enum<?>)val).name();

        if (val instanceof ClientListenerProtocolVersion) {
            ClientListenerProtocolVersion ver = (ClientListenerProtocolVersion)val;

            return ver.major() + "." + ver.minor() + '.' + ver.maintenance();
        }

        if (val != null && val.getClass().isArray()) {
            List<String> items = new ArrayList<>();

            for (int i = 0; i < Array.getLength(val); i++)
                items.add(valueText(Array.get(val, i)));

            return '{' + String.join(", ", items) + '}';
        }

        return String.valueOf(val);
    }

    /** Describes the system properties that {@code ignite.sh -systemProps} prints, without their descriptions. */
    private static void systemProperties() throws ReflectiveOperationException {
        Field propsCls = CommandLineStartup.class.getDeclaredField("PROPS_CLS");

        propsCls.setAccessible(true);

        for (Class<?> cls : (Collection<Class<?>>)propsCls.get(null)) {
            for (Field f : cls.getFields()) {
                SystemProperty prop = f.getAnnotation(SystemProperty.class);

                if (prop == null)
                    continue;

                boolean deprecated = f.isAnnotationPresent(Deprecated.class);
                String dflt = prop.type() == Boolean.class && prop.defaults().isEmpty() ? "false" : prop.defaults();

                add("Configuration", "system property " + f.get(null), prop.type().getSimpleName()
                    + (dflt.isEmpty() ? "" : ", default " + dflt) + (deprecated ? ", deprecated" : ""),
                    flags(deprecated, false), where(cls, f.getName(), -1, deprecated));
            }
        }
    }

    /**
     * Describes system view columns in the order that the generated walkers give them to clients. The class of the view row
     * is a rename group: the SQL name of the view is registered at runtime, and renaming the class changes nothing.
     */
    private static void systemViews(List<Path> classDirs) throws Exception {
        int walkers = 0;

        for (Class<?> cls : classes(classDirs, SystemViewRowAttributeWalkerGenerator.WALKER_PACKAGE, false)) {
            if (cls.isInterface() || !SystemViewRowAttributeWalker.class.isAssignableFrom(cls))
                continue;

            Class<?> row = (Class<?>)Arrays.stream(cls.getGenericInterfaces())
                .filter(t -> t instanceof ParameterizedType && ((ParameterizedType)t).getRawType() == SystemViewRowAttributeWalker.class)
                .map(t -> ((ParameterizedType)t).getActualTypeArguments()[0]).findFirst().orElseThrow();

            walkers++;

            ((SystemViewRowAttributeWalker<?>)cls.getDeclaredConstructor().newInstance()).visitAll(
                new SystemViewRowAttributeWalker.AttributeVisitor() {
                    @Override public <T> void accept(int idx, String name, Class<T> type) {
                        add("Monitoring", "system view " + row.getSimpleName() + " column " + name,
                            "position " + (idx + 1) + ", " + type.getTypeName(), "", where(row, name, 0, false), row.getSimpleName());
                    }
                });
        }

        if (walkers == 0)
            errors.add("No system view walkers found in " + SystemViewRowAttributeWalkerGenerator.WALKER_PACKAGE);
    }

    /** Describes {@code control.sh} commands, their parameters and the common options. */
    private static void cli() throws ReflectiveOperationException {
        IgniteCommandRegistry registry = new IgniteCommandRegistry();

        registry.commands().forEachRemaining(e -> command(e.getValue(), new ArrayList<>()));

        Field common = ArgumentParser.class.getDeclaredField("common");

        common.setAccessible(true);

        for (CLIArgument<?> arg : (List<CLIArgument<?>>)common.get(new ArgumentParser(null, registry, null))) {
            add("CLI", "control.sh " + arg.name(), "common option, " + (arg.optional() ? "optional" : "required") + ", "
                + arg.type().getTypeName() + (arg.isSensitive() ? ", sensitive" : ""), "", where(ArgumentParser.class, null, -1, false));
        }
    }

    /**
     * Describes a command and its subcommands. {@code control.sh} refuses experimental commands unless
     * {@code --enable-experimental} is given, so the experimental mark is part of the command.
     */
    private static void command(Command<?, ?> cmd, List<Command<?, ?>> parents) {
        CommandHandler.CommandName name = new CommandHandler.CommandName();

        parents.forEach(name);
        name.accept(cmd);

        String path = "control.sh" + commandName(name);
        boolean experimental = cmd.getClass().isAnnotationPresent(IgniteExperimental.class);
        boolean deprecated = cmd.getClass().isAnnotationPresent(Deprecated.class)
            || parents.stream().anyMatch(p -> p.getClass().isAnnotationPresent(Deprecated.class));
        String flags = flags(deprecated, experimental || parents.stream().anyMatch(p -> p.getClass().isAnnotationPresent(IgniteExperimental.class)));

        if (CommandUtils.executable(cmd)) {
            add("CLI", path, "command" + (experimental ? ", experimental" : "") + (deprecated ? ", deprecated" : ""),
                flags, where(cmd.getClass(), null, -1, deprecated));

            int[] pos = {0};

            CommandUtils.visitCommandParams(cmd.argClass(),
                fld -> parameter(path, fld, ++pos[0], false, flags),
                fld -> parameter(path, fld, 0, false, flags),
                (grp, flds) -> {
                    List<String> names = new ArrayList<>();

                    for (Field fld : flds) {
                        boolean positional = fld.isAnnotationPresent(Positional.class);

                        names.add(positional ? "<arg " + (pos[0] + 1) + '>' : CommandUtils.toFormattedFieldName(fld));
                        parameter(path, fld, positional ? ++pos[0] : 0, true, flags);
                    }

                    add("CLI", path + ' ' + String.join("|", names), "group, " + (grp.optional() ? "optional" : "required")
                        + (grp.onlyOneOf() ? ", only one of" : ""), flags, where(cmd.argClass(), null, -1, false));
                });
        }

        if (cmd instanceof CommandsRegistry) {
            List<Command<?, ?>> parents0 = new ArrayList<>(parents);

            parents0.add(cmd);

            ((CommandsRegistry<?, ?>)cmd).commands().forEachRemaining(e -> command(e.getValue(), parents0));
        }
    }

    /** @return Command name as {@code control.sh} prints it in the help. */
    private static String commandName(CommandHandler.CommandName name) {
        try {
            Field fld = CommandHandler.CommandName.class.getDeclaredField("name");

            fld.setAccessible(true);

            return fld.get(name).toString();
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Describes a command parameter. Positional parameters are identified by position, named ones by name. The parser treats
     * a named parameter of a group as optional and checks the group instead, so such a parameter is described as part of
     * its group.
     */
    private static void parameter(String path, Field fld, int pos, boolean grouped, String flags) {
        Argument arg = fld.getAnnotation(Argument.class);
        Class<?> type = fld.getType().isArray() ? fld.getType().getComponentType() : fld.getType();

        String values = type.isEnum()
            ? Arrays.stream(type.getEnumConstants()).map(c -> ((Enum<?>)c).name()).collect(Collectors.joining("|", ", values ", ""))
            : "";
        String required = pos == 0 && grouped ? "in a group, " : arg != null && arg.optional() ? "optional, " : "required, ";

        add("CLI", path + ' ' + (pos > 0 ? "<arg " + pos + '>' : CommandUtils.toFormattedFieldName(fld)),
            required + fld.getType().getTypeName() + values + (arg != null && arg.sensitive() ? ", sensitive" : ""), flags,
            where(fld.getDeclaringClass(), fld.getName(), -1, false));
    }

    /**
     * Describes protocol, storage and error constants declared in internal classes. The name of a constant is a rename group:
     * users see only its value.
     */
    private static void constants() throws ReflectiveOperationException {
        for (String[] c : CONSTANTS) {
            Class<?> cls = Class.forName(c[1]);
            int found = 0;

            for (Field f : cls.getDeclaredFields()) {
                int mod = f.getModifiers();
                Class<?> type = f.getType();

                if (f.isSynthetic() || !Modifier.isStatic(mod) || !Modifier.isFinal(mod) || !f.getName().matches(c[2])
                    || !(type.isPrimitive() || type == String.class || type == ClientListenerProtocolVersion.class))
                    continue;

                boolean deprecated = f.isAnnotationPresent(Deprecated.class);
                String key = cls.getSimpleName() + '.' + f.getName();
                Object val = staticValue(f);

                add(c[0], key, valueText(val), flags(deprecated, false), where(cls, f.getName(), -1, deprecated), key);

                if (cls == IgniteQueryErrorCode.class && type == int.class) {
                    add(c[0], "SQLSTATE of query error code " + val, IgniteQueryErrorCode.codeToSqlState((Integer)val), "",
                        where(cls, "codeToSqlState", 1, false));
                }

                found++;
            }

            if (found == 0)
                errors.add("No constants matching " + c[2] + " found in " + c[1]);
        }

        for (String[] c : CODED_ENUMS) {
            Class<?> cls = Class.forName(c[1]);
            Method code = cls.getMethod(c[2]);

            for (Object e : cls.getEnumConstants()) {
                String name = ((Enum<?>)e).name();
                boolean deprecated = cls.getField(name).isAnnotationPresent(Deprecated.class);
                String key = cls.getSimpleName() + '.' + name;

                add(c[0], key, String.valueOf(code.invoke(e)), flags(deprecated, false), where(cls, name, -1, deprecated), key);
            }
        }
    }

    /**
     * Describes the keywords of the Calcite SQL parser and the operators and functions of the Calcite engine. Each allowed
     * signature is part of the key, so the forms of an overloaded function are compared separately.
     */
    private static void sql() {
        SqlAbstractParserImpl parser = IgniteSqlParserImpl.FACTORY.getParser(new StringReader(""));

        for (String token : parser.getMetadata().getTokens()) {
            add("SQL keywords", "Calcite " + token, parser.getMetadata().isReservedWord(token) ? "reserved" : "non-reserved", "",
                where(IgniteSqlParserImpl.class, null, -1, false));
        }

        for (ReflectiveSqlOperatorTable t : List.of(IgniteStdSqlOperatorTable.INSTANCE, IgniteOwnSqlOperatorTable.instance())) {
            for (SqlOperator op : t.getOperatorList()) {
                if (op.getName().startsWith("$"))
                    continue;

                String signatures;

                try {
                    signatures = op.getAllowedSignatures();
                }
                catch (RuntimeException e) {
                    signatures = "";
                }

                add("SQL functions", op.getName() + ' ' + op.getSyntax() + ' ' + signatures, "", "",
                    where(t.getClass(), null, -1, false));
            }
        }
    }

    /** Describes runtime requirements and packages declared in build and launch files. */
    private static void distribution() throws IOException {
        String parent = "parent/pom.xml";

        add("Distribution", "maven.compiler.release", tag(Files.readString(root.resolve(parent)), "maven.compiler.release"), "", parent);

        String launcher = "bin/include/functions.sh";
        Matcher minJava = Pattern.compile("\\$version -lt (\\d+)").matcher(Files.readString(root.resolve(launcher)));

        if (!minJava.find())
            errors.add("No minimum Java version check found in " + launcher);
        else
            add("Distribution", "minimum Java version in launch scripts", minJava.group(1), "", launcher);

        try (Stream<Path> files = Files.walk(root.resolve("modules/platforms/dotnet"))) {
            for (Path f : (Iterable<Path>)files.filter(p -> p.toString().endsWith(".csproj"))::iterator) {
                String xml = Files.readString(f);
                String id = tag(xml, "PackageId");

                if (id == null)
                    continue;

                String frameworks = tag(xml, "TargetFrameworks");

                add("Distribution", "NuGet package " + id, frameworks != null ? frameworks : tag(xml, "TargetFramework"), "",
                    root.relativize(f).toString());
            }
        }
    }
}
