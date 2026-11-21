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
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
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
import org.apache.ignite.internal.management.api.CliConfirmArgument;
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
 * Prints the public declarations of a built Ignite source tree, one per line: area, key, value, flags and source location,
 * separated by tabs. Flags mark deprecated and experimental declarations. Descriptions, help texts and examples are left out,
 * so editing them changes nothing.
 * <p>
 * Usage: {@code java -cp <classpath of the built tree> PublicDeclarations.java <tree root>}.
 */
@SuppressWarnings("unchecked")
public class PublicDeclarations {
    /** */
    private static final String DEPRECATED = "deprecated";

    /** */
    private static final String EXPERIMENTAL = "experimental";

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

    /** Output lines. */
    private static final Set<String> lines = new TreeSet<>();

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

        lines.forEach(System.out::println);

        if (!errors.isEmpty()) {
            errors.forEach(System.err::println);

            System.exit(1);
        }
    }

    /** Adds a declaration. */
    private static void add(String area, String key, String val, String flags, String location) {
        lines.add(String.join("\t", area, oneLine(key), oneLine(val), flags, location));
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
     * @param deprecated Whether to point at the {@code @Deprecated} annotation of the declaration.
     * @return Source file and line of the declaration, as precise as a text search allows, or an empty string.
     */
    private static String where(Class<?> cls, String member, boolean deprecated) {
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

        int type = find(text, 0, "\\b(class|interface|enum|@interface)\\s+" + Pattern.quote(cls.getSimpleName()) + "\\b");
        int line = member == null ? type : find(text, Math.max(type, 0), "\\b" + Pattern.quote(member) + "\\b\\s*[(=;,]");

        if (line < 0)
            line = type;

        if (line < 0)
            return root.relativize(src).toString();

        for (int i = line; deprecated && i >= 0 && line - i < 20; i--) {
            if (text.get(i).contains("@Deprecated")) {
                line = i;

                break;
            }

            String l = text.get(i).trim();

            if (i < line && (l.endsWith(";") || l.endsWith("}")))
                break;
        }

        return root.relativize(src) + ":" + (line + 1);
    }

    /** @return Index of the first line from {@code from} that matches the pattern, or -1. */
    private static int find(List<String> text, int from, String regex) {
        Pattern p = Pattern.compile(regex);

        for (int i = from; i < text.size(); i++) {
            if (p.matcher(text.get(i)).find())
                return i;
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
                if (apiType(cls) && !where(cls, null, false).isEmpty()) {
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

    /** Describes a public type and the members that users can call or extend. */
    private static void javaType(Class<?> cls) {
        String name = cls.getCanonicalName();
        boolean deprecated = marked(cls, cls.getDeclaringClass(), Deprecated.class);
        boolean experimental = marked(cls, cls.getDeclaringClass(), IgniteExperimental.class);

        Set<String> supers = new TreeSet<>();

        collectSupertypes(cls, supers);

        String kind = cls.isAnnotation() ? "@interface" : cls.isInterface() ? "interface" : cls.isEnum() ? "enum" : "class";
        int mask = Modifier.PUBLIC | Modifier.PROTECTED | Modifier.STATIC
            | (cls.isInterface() || cls.isEnum() ? 0 : Modifier.FINAL | Modifier.ABSTRACT);

        add("Java API", name, markers(cls) + modifiers(cls.getModifiers() & mask) + kind + ' ' + cls.getSimpleName()
            + typeParams(cls.getTypeParameters()) + (supers.isEmpty() ? "" : " : " + String.join(", ", supers)),
            flags(deprecated, experimental), where(cls, null, deprecated));

        Map<String, Member> members = new HashMap<>();

        describeMembers(cls, cls, members);

        Set<Class<?>> internalSupers = new LinkedHashSet<>();

        collectInternalSupertypes(cls, internalSupers);

        for (Class<?> sup : internalSupers)
            describeMembers(sup, cls, members);

        members.forEach((key, m) -> {
            boolean dep = deprecated || m.elem.isAnnotationPresent(Deprecated.class);
            boolean exp = experimental || marked(m.elem, m.owner, IgniteExperimental.class);

            add("Java API", name + '#' + key, m.text, flags(dep, exp), where(m.owner, m.name, dep));
        });
    }

    /** Member of a public type. */
    private static class Member {
        /** */
        final AnnotatedElement elem;

        /** Class that declares the member. */
        final Class<?> owner;

        /** Name in the source. */
        final String name;

        /** Description. */
        final String text;

        /** */
        Member(AnnotatedElement elem, Class<?> owner, String name, String text) {
            this.elem = elem;
            this.owner = owner;
            this.name = name;
            this.text = text;
        }
    }

    /** Collects public supertypes, looking through Ignite internal ones. */
    private static void collectSupertypes(Class<?> cls, Set<String> res) {
        List<Type> direct = new ArrayList<>(Arrays.asList(cls.getGenericInterfaces()));

        if (cls.getGenericSuperclass() != null && cls.getSuperclass() != Object.class && !cls.isEnum())
            direct.add(cls.getGenericSuperclass());

        for (Type t : direct) {
            Class<?> raw = t instanceof ParameterizedType ? (Class<?>)((ParameterizedType)t).getRawType() : (Class<?>)t;

            if (internalType(raw))
                collectSupertypes(raw, res);
            else if (raw != java.lang.annotation.Annotation.class)
                res.add(t.getTypeName());
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
    private static void describeMembers(Class<?> src, Class<?> api, Map<String, Member> res) {
        for (Constructor<?> c : src == api ? src.getDeclaredConstructors() : new Constructor<?>[0]) {
            if (!c.isSynthetic() && accessible(c.getModifiers(), api)) {
                res.putIfAbsent("<init>" + params(c),
                    new Member(c, src, api.getSimpleName(), markers(c) + modifiers(c.getModifiers()) + api.getSimpleName() + signature(c)));
            }
        }

        for (Method m : src.getDeclaredMethods()) {
            if (m.isSynthetic() || m.isBridge() || !accessible(m.getModifiers(), api))
                continue;

            String dflt = m.getDefaultValue() == null ? "" : " default " + valueText(m.getDefaultValue());

            res.putIfAbsent(m.getName() + params(m), new Member(m, src, m.getName(), markers(m) + modifiers(m.getModifiers())
                + (m.isDefault() ? "default " : "") + typeParams(m.getTypeParameters()) + m.getGenericReturnType().getTypeName()
                + ' ' + m.getName() + signature(m) + dflt));
        }

        for (Field f : src.getDeclaredFields()) {
            if (f.isSynthetic() || !accessible(f.getModifiers(), api))
                continue;

            res.putIfAbsent(f.getName(), new Member(f, src, f.getName(), markers(f) + modifiers(f.getModifiers())
                + f.getGenericType().getTypeName() + ' ' + f.getName() + constantValue(f)));
        }
    }

    /** @return Erased parameter types that identify an overload. */
    private static String params(Executable e) {
        return Arrays.stream(e.getParameterTypes()).map(Class::getTypeName).collect(Collectors.joining(",", "(", ")"));
    }

    /** @return Generic parameter and exception types. */
    private static String signature(Executable e) {
        Type[] params = e.getGenericParameterTypes();
        List<String> names = new ArrayList<>();

        for (int i = 0; i < params.length; i++) {
            String n = params[i].getTypeName();

            names.add(e.isVarArgs() && i == params.length - 1 ? n.substring(0, n.length() - 2) + "..." : n);
        }

        Type[] exceptions = e.getGenericExceptionTypes();

        return '(' + String.join(", ", names) + ')' + (exceptions.length == 0 ? "" :
            Arrays.stream(exceptions).map(Type::getTypeName).sorted().collect(Collectors.joining(", ", " throws ", "")));
    }

    /** @return Type parameters with their bounds. */
    private static String typeParams(TypeVariable<?>[] vars) {
        if (vars.length == 0)
            return "";

        return Arrays.stream(vars).map(v -> {
            List<String> bounds = Arrays.stream(v.getBounds()).filter(b -> b != Object.class).map(Type::getTypeName)
                .collect(Collectors.toList());

            return v.getName() + (bounds.isEmpty() ? "" : " extends " + String.join(" & ", bounds));
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
                    flags(deprecated, false), where(cls, f.getName(), deprecated));
            }
        }
    }

    /** Describes system view columns in the order that the generated walkers give them to clients. */
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
                            "position " + (idx + 1) + ", " + type.getTypeName(), "", where(row, name, false));
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
                + arg.type().getTypeName() + (arg.isSensitive() ? ", sensitive" : ""), "", where(ArgumentParser.class, null, false));
        }
    }

    /** Describes a command and its subcommands. */
    private static void command(Command<?, ?> cmd, List<Command<?, ?>> parents) {
        CommandHandler.CommandName name = new CommandHandler.CommandName();

        parents.forEach(name);
        name.accept(cmd);

        String path = "control.sh" + commandName(name);
        boolean experimental = cmd.getClass().isAnnotationPresent(IgniteExperimental.class)
            || parents.stream().anyMatch(p -> p.getClass().isAnnotationPresent(IgniteExperimental.class));
        String flags = flags(false, experimental);

        if (CommandUtils.executable(cmd)) {
            add("CLI", path, "command" + (cmd.argClass().isAnnotationPresent(CliConfirmArgument.class) ? ", asks for confirmation" : ""),
                flags, where(cmd.getClass(), null, false));

            int[] pos = {0};

            CommandUtils.visitCommandParams(cmd.argClass(),
                fld -> parameter(path, fld, ++pos[0], flags),
                fld -> parameter(path, fld, 0, flags),
                (grp, flds) -> {
                    List<String> names = new ArrayList<>();

                    for (Field fld : flds) {
                        boolean positional = fld.isAnnotationPresent(Positional.class);

                        names.add(positional ? "<arg " + (pos[0] + 1) + '>' : CommandUtils.toFormattedFieldName(fld));
                        parameter(path, fld, positional ? ++pos[0] : 0, flags);
                    }

                    add("CLI", path + ' ' + String.join("|", names), "group, " + (grp.optional() ? "optional" : "required")
                        + (grp.onlyOneOf() ? ", only one of" : ""), flags, where(cmd.argClass(), null, false));
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

    /** Describes a command parameter. Positional parameters are identified by position, named ones by name. */
    private static void parameter(String path, Field fld, int pos, String flags) {
        Argument arg = fld.getAnnotation(Argument.class);
        Class<?> type = fld.getType().isArray() ? fld.getType().getComponentType() : fld.getType();

        String values = type.isEnum()
            ? Arrays.stream(type.getEnumConstants()).map(c -> ((Enum<?>)c).name()).collect(Collectors.joining("|", ", values ", ""))
            : "";

        add("CLI", path + ' ' + (pos > 0 ? "<arg " + pos + '>' : CommandUtils.toFormattedFieldName(fld)),
            (arg != null && arg.optional() ? "optional, " : "required, ") + fld.getType().getTypeName() + values
                + (arg != null && arg.sensitive() ? ", sensitive" : ""), flags, where(fld.getDeclaringClass(), fld.getName(), false));
    }

    /** Describes protocol, storage and error constants declared in internal classes. */
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

                add(c[0], cls.getSimpleName() + '.' + f.getName(), valueText(staticValue(f)), flags(deprecated, false),
                    where(cls, f.getName(), deprecated));

                if (cls == IgniteQueryErrorCode.class && type == int.class) {
                    add(c[0], "SQLState of IgniteQueryErrorCode." + f.getName(),
                        IgniteQueryErrorCode.codeToSqlState((Integer)staticValue(f)), "", where(cls, "codeToSqlState", false));
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

                add(c[0], cls.getSimpleName() + '.' + name, String.valueOf(code.invoke(e)), flags(deprecated, false),
                    where(cls, name, deprecated));
            }
        }
    }

    /** Describes the keywords of the Calcite SQL parser and the operators and functions of the Calcite engine. */
    private static void sql() {
        SqlAbstractParserImpl parser = IgniteSqlParserImpl.FACTORY.getParser(new StringReader(""));

        for (String token : parser.getMetadata().getTokens()) {
            add("SQL keywords", "Calcite " + token, parser.getMetadata().isReservedWord(token) ? "reserved" : "non-reserved", "",
                where(IgniteSqlParserImpl.class, null, false));
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

                add("SQL functions", op.getName() + ' ' + op.getSyntax(), signatures, "", where(t.getClass(), op.getName(), false));
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
