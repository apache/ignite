/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.sun.source.tree.*;
import com.sun.source.util.JavacTask;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import javax.lang.model.element.Modifier;
import javax.tools.*;

class JavaContracts {
    private record Entry(Path file, String owner, String subject, String signature, boolean couldBeInherited, boolean deprecated) {}

    private static final Set<String> OBJECT_METHODS = Set.of("equals", "hashCode", "toString", "getClass", "clone",
        "finalize", "notify", "notifyAll", "wait");

    private static final Map<String, String> WIRE_TYPES = Map.of(
        "org.apache.ignite.internal.processors.odbc.SqlStateCode", "SQLState",
        "org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode", "SQL error code",
        "org.apache.ignite.internal.client.thin.ClientOperation", "client operation ID",
        "org.apache.ignite.internal.processors.platform.client.ClientBitmaskFeature", "client feature ID");

    private final Map<String, List<Entry>> declarations = new TreeMap<>();
    private final Map<String, Map<String, String>> wireValues = new TreeMap<>();
    private final Map<String, Path> wireFiles = new HashMap<>();
    private final Set<String> typesWithParents = new HashSet<>();

    private static boolean exported(ModifiersTree mods, boolean implicitPublic) {
        Set<Modifier> flags = mods.getFlags();
        return !flags.contains(Modifier.PRIVATE) && (implicitPublic || flags.contains(Modifier.PUBLIC)
            || flags.contains(Modifier.PROTECTED));
    }

    private static Object literal(ExpressionTree expr) {
        if (expr instanceof ParenthesizedTree parens)
            return literal(parens.getExpression());
        if (expr instanceof LiteralTree lit && lit.getValue() != null)
            return lit.getValue();
        if (expr instanceof UnaryTree unary && unary.getKind() == Tree.Kind.UNARY_MINUS
            && unary.getExpression() instanceof LiteralTree lit && lit.getValue() instanceof Number)
            return switch (lit.getKind()) {
                case INT_LITERAL -> -((Integer)lit.getValue());
                case LONG_LITERAL -> -((Long)lit.getValue());
                case FLOAT_LITERAL -> -((Float)lit.getValue());
                case DOUBLE_LITERAL -> -((Double)lit.getValue());
                default -> null;
            };
        return null;
    }

    private static String constantValue(VariableTree field) {
        Object value = literal(field.getInitializer());
        if (value == null)
            return null;
        String type = field.getType().toString();
        if (Set.of("String", "java.lang.String").contains(type) && value instanceof String text)
            return text;
        if (type.equals("boolean") && value instanceof Boolean flag)
            return flag.toString();
        if (value instanceof Character ch)
            value = (int)ch;
        if (!(value instanceof Number number))
            return null;
        return switch (type) {
            case "double" -> Double.toString(number.doubleValue());
            case "float" -> Float.toString(number.floatValue());
            case "byte", "short", "char", "int", "long" -> number instanceof Integer || number instanceof Long
                ? Long.toString(number.longValue()) : null;
            default -> null;
        };
    }

    // Qualified and imported spellings of the same type share its simple name. A change of imports alone
    // is outside this comparison. Generic type variables need attribution, so leave their signatures unknown.
    private static String type(Tree type, Set<String> variables) {
        if (type instanceof AnnotatedTypeTree annotated)
            return type(annotated.getUnderlyingType(), variables);
        if (type instanceof ParameterizedTypeTree parameterized)
            return type(parameterized.getType(), variables);
        if (type instanceof ArrayTypeTree array) {
            String element = type(array.getType(), variables);
            return element == null ? null : element + "[]";
        }
        if (type instanceof PrimitiveTypeTree primitive)
            return primitive.getPrimitiveTypeKind().toString().toLowerCase(Locale.ROOT);
        if (type instanceof MemberSelectTree member)
            return member.getIdentifier().toString();
        if (type instanceof IdentifierTree id && !variables.contains(id.getName().toString()))
            return id.getName().toString();
        return null;
    }

    private static boolean annotated(ModifiersTree mods, String type) {
        return mods.getAnnotations().stream().map(a -> a.getAnnotationType().toString())
            .anyMatch(name -> name.equals(type) || name.equals("java.lang." + type));
    }

    private static String visibility(ModifiersTree mods) {
        return mods.getFlags().contains(Modifier.PROTECTED) ? "protected" : "public";
    }

    private void add(String key, Entry entry) {
        declarations.computeIfAbsent(key, ignored -> new ArrayList<>()).add(entry);
    }

    private void readType(Path file, String owner, ClassTree cls, boolean implicitPublic, boolean ownerDeprecated,
        Set<String> outerVariables) {
        String name = owner.isEmpty() ? cls.getSimpleName().toString() : owner + "." + cls.getSimpleName();
        if (WIRE_TYPES.containsKey(name)) {
            readWireValues(file, name, cls);
            return;
        }
        if (name.contains(".internal.") || !exported(cls.getModifiers(), implicitPublic))
            return;

        boolean iface = cls.getKind() == Tree.Kind.INTERFACE || cls.getKind() == Tree.Kind.ANNOTATION_TYPE;
        boolean hasParents = cls.getExtendsClause() != null || !cls.getImplementsClause().isEmpty();
        if (hasParents)
            typesWithParents.add(name);
        Set<String> variables = new HashSet<>(outerVariables);
        cls.getTypeParameters().forEach(t -> variables.add(t.getName().toString()));
        boolean deprecated = ownerDeprecated || annotated(cls.getModifiers(), "Deprecated");
        String kind = cls.getKind().toString().toLowerCase(Locale.ROOT);
        add(name, new Entry(file, owner, "API type " + name, visibility(cls.getModifiers()) + " " + kind, false, deprecated));

        for (Tree member : cls.getMembers()) {
            if (member instanceof ClassTree nested)
                readType(file, name, nested, iface, deprecated, variables);
            else if (member instanceof MethodTree method && method.getReturnType() != null
                && cls.getKind() != Tree.Kind.RECORD && exported(method.getModifiers(), iface)) {
                String methodName = method.getName().toString();
                boolean inherited = hasParents || OBJECT_METHODS.contains(methodName)
                    || cls.getKind() == Tree.Kind.ENUM || annotated(method.getModifiers(), "Override");
                Set<String> methodVars = new HashSet<>(variables);
                method.getTypeParameters().forEach(t -> methodVars.add(t.getName().toString()));
                List<String> params = new ArrayList<>();
                for (VariableTree param : method.getParameters())
                    params.add(type(param.getType(), methodVars));
                String returns = type(method.getReturnType(), methodVars);
                String access = visibility(method.getModifiers());
                if (method.getModifiers().getFlags().contains(Modifier.STATIC))
                    access += " static";
                String signature = returns == null || params.contains(null) ? "?" :
                    access + " (" + String.join(", ", params) + ") -> " + returns;
                String key = name + "." + methodName + "/" + params.size();
                String arity = params.size() + (params.size() == 1 ? " parameter" : " parameters");
                add(key, new Entry(file, name, "API method " + name + "." + methodName + " (" + arity + ")", signature,
                    inherited, deprecated || annotated(method.getModifiers(), "Deprecated")));
            }
            else if (member instanceof VariableTree var && exported(var.getModifiers(), iface)
                && (iface || var.getModifiers().getFlags().containsAll(Set.of(Modifier.FINAL, Modifier.STATIC)))) {
                String key = name + "." + var.getName();
                add(key + " field", new Entry(file, name, "API constant " + key, constantValue(var), false,
                    deprecated || annotated(var.getModifiers(), "Deprecated")));
            }
        }
    }

    private void readWireValues(Path path, String name, ClassTree cls) {
        Map<String, String> values = new TreeMap<>();
        wireFiles.put(name, path);
        wireValues.put(name, values);
        for (Tree member : cls.getMembers()) {
            if (!(member instanceof VariableTree var))
                continue;
            String value;
            if (cls.getKind() == Tree.Kind.ENUM) {
                // Enum constants carry their wire ID in the constructor arguments.
                if (!(var.getInitializer() instanceof NewClassTree init)
                    || !var.getType().toString().equals(cls.getSimpleName().toString()))
                    continue;
                Object id = init.getArguments().size() == 1 ? literal(init.getArguments().get(0)) : null;
                value = id instanceof Integer n ? n.toString() : null;
            }
            else {
                boolean sqlState = name.endsWith(".SqlStateCode");
                String fieldType = var.getType().toString();
                if (!var.getModifiers().getFlags().containsAll(Set.of(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL))
                    || (sqlState ? !Set.of("String", "java.lang.String").contains(fieldType) : !fieldType.equals("int")))
                    continue;
                value = constantValue(var);
            }
            // An expression can have the same value as a literal in the other revision.
            values.put(var.getName().toString(), value);
        }
    }

    private static JavaContracts read(Path root, Set<String> invalidFiles) throws Exception {
        JavaContracts snapshot = new JavaContracts();
        List<Path> paths;
        try (var walk = Files.walk(root)) {
            paths = walk.filter(p -> p.toString().endsWith(".java")).sorted().toList();
        }
        if (paths.isEmpty())
            return snapshot;
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> problems = new DiagnosticCollector<>();
        try (var fs = compiler.getStandardFileManager(problems, Locale.ROOT, StandardCharsets.UTF_8)) {
            JavacTask task = (JavacTask)compiler.getTask(null, fs, problems, List.of("-proc:none"), null,
                fs.getJavaFileObjectsFromPaths(paths));
            Iterable<? extends CompilationUnitTree> units = task.parse();
            for (var diagnostic : problems.getDiagnostics()) {
                if (diagnostic.getKind() == Diagnostic.Kind.ERROR) {
                    if (diagnostic.getSource() == null)
                        throw new IllegalStateException("Could not parse the Java snapshots.");
                    String file = Path.of(diagnostic.getSource().toUri()).getFileName().toString();
                    invalidFiles.add(file);
                }
            }
            for (CompilationUnitTree unit : units) {
                Path path = root.relativize(Path.of(unit.getSourceFile().toUri()));
                if (invalidFiles.contains(path.getFileName().toString()))
                    continue;
                String pkg = unit.getPackageName() == null ? "" : unit.getPackageName().toString();
                for (Tree top : unit.getTypeDecls()) {
                    if (top instanceof ClassTree cls)
                        snapshot.readType(path, pkg, cls, false, false, Set.of());
                }
            }
        }
        return snapshot;
    }

    private static void emit(Path path, String text) {
        // A TSV row must keep its file and message on the same line.
        if (path.toString().chars().noneMatch(c -> Character.isISOControl(c) || c == 0x2028 || c == 0x2029))
            System.out.println(path + "\t" + text.replaceAll("[\\p{Cc}\\p{Zl}\\p{Zp}]", " "));
    }

    private static void compareWire(String name, JavaContracts before, JavaContracts after, Set<String> invalid) {
        Path path = after.wireFiles.getOrDefault(name, before.wireFiles.get(name));
        if (invalid.contains(path.getFileName().toString()))
            return;
        Map<String, String> old = before.wireValues.getOrDefault(name, Map.of());
        Map<String, String> now = after.wireValues.getOrDefault(name, Map.of());
        Set<String> removed = new TreeSet<>();
        Set<String> added = new TreeSet<>();
        // Unknown expressions may supply a value missing from the other revision's list of literals.
        if (now.values().stream().noneMatch(Objects::isNull))
            old.values().stream().filter(v -> v != null && !now.containsValue(v)).forEach(removed::add);
        if (old.values().stream().noneMatch(Objects::isNull))
            now.values().stream().filter(v -> v != null && !old.containsValue(v)).forEach(added::add);
        String subject = WIRE_TYPES.get(name);
        for (var entry : now.entrySet()) {
            String from = old.get(entry.getKey());
            if (from != null && entry.getValue() != null && !from.equals(entry.getValue())) {
                emit(path, "Changed " + subject + " for " + entry.getKey() + " from " + from + " to " + entry.getValue());
                removed.remove(from);
                added.remove(entry.getValue());
            }
        }
        removed.forEach(v -> emit(path, "Removed " + subject + " " + v));
        added.forEach(v -> emit(path, "Added " + subject + " " + v));
    }

    public static void main(String[] args) throws Exception {
        Set<String> invalid = new HashSet<>();
        JavaContracts before = read(Path.of(args[0]).toAbsolutePath(), invalid);
        JavaContracts after = read(Path.of(args[1]).toAbsolutePath(), invalid);
        Set<String> keys = new TreeSet<>(before.declarations.keySet());
        keys.addAll(after.declarations.keySet());
        for (String key : keys) {
            List<Entry> old = before.declarations.getOrDefault(key, List.of());
            List<Entry> now = after.declarations.getOrDefault(key, List.of());
            List<Entry> both = new ArrayList<>(old);
            both.addAll(now);
            if (both.stream().anyMatch(e -> invalid.contains(e.file.getFileName().toString())
                || e.signature == null || e.couldBeInherited))
                continue;
            Entry item = now.isEmpty() ? old.get(0) : now.get(0);
            if (old.size() != now.size()
                && (before.typesWithParents.contains(item.owner) || after.typesWithParents.contains(item.owner)))
                continue;
            if (old.size() < now.size())
                emit(item.file, "Added " + item.subject);
            else if (old.size() > now.size()) {
                // Previously deprecated APIs may be removed under the project's release rules.
                if (old.stream().filter(e -> !e.deprecated).count() > now.size())
                    emit(item.file, "Removed " + item.subject);
            }
            else if (both.stream().noneMatch(e -> e.signature.equals("?"))) {
                List<String> from = old.stream().map(Entry::signature).sorted().toList();
                List<String> to = now.stream().map(Entry::signature).sorted().toList();
                if (!from.equals(to))
                    emit(item.file, "Changed " + item.subject + " from " + String.join(", ", from) + " to " + String.join(", ", to));
            }
        }
        Set<String> wireTypes = new TreeSet<>(before.wireValues.keySet());
        wireTypes.addAll(after.wireValues.keySet());
        for (String name : wireTypes)
            compareWire(name, before, after, invalid);
    }
}
