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
import com.sun.source.util.TreeScanner;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiConsumer;
import javax.lang.model.element.Modifier;

/** Compare the indexed columns exposed by registered, generated system view walkers. */
class SystemViewColumns {
    private static final String GENERATOR = "org.apache.ignite.internal.systemview.";
    private static final String ROWS = "org.apache.ignite.spi.systemview.view.";
    private static final String MANAGERS = "org.apache.ignite.internal.managers.systemview.";
    private static final Path MANAGER_PATH = Path.of("modules/core/src/main/java/org/apache/ignite/internal/managers/systemview");
    private static final String STATS = "org.apache.ignite.internal.processors.query.stat.view.";
    private static final String CODEGEN = "modules/codegen/src/main/java/org/apache/ignite/internal/systemview/";
    private static final Set<String> REGISTRATIONS = Set.of("registerView", "registerFiltrableView", "registerInnerCollectionView");

    private record Row(Path file, CompilationUnitTree unit, ClassTree type) {}
    private record Column(String name, Integer order, Path file) {}

    private static boolean imported(CompilationUnitTree unit, String name) {
        return unit.getImports().stream().anyMatch(i -> i.getQualifiedIdentifier().toString().equals(name));
    }

    private static boolean named(CompilationUnitTree unit, Tree ref, String name) {
        return ref.toString().equals(name) || ref.toString().equals(name.substring(name.lastIndexOf('.') + 1)) && imported(unit, name);
    }

    private static Map<String, Row> rows(Map<Path, CompilationUnitTree> units) {
        Map<String, Row> rows = new HashMap<>();
        units.forEach((path, unit) -> {
            String pkg = unit.getPackageName() + ".";
            if (pkg.startsWith(ROWS) || pkg.equals(STATS)) {
                for (Tree declaration : unit.getTypeDecls()) {
                    if (declaration instanceof ClassTree cls && cls.getKind() == Tree.Kind.CLASS
                        && path.endsWith((pkg + cls.getSimpleName()).replace('.', '/') + ".java"))
                        rows.put(pkg + cls.getSimpleName(), new Row(path, unit, cls));
                }
            }
        });
        return rows;
    }

    private static String parent(Row row) {
        Tree ref = row.type.getExtendsClause();
        if (ref == null)
            return "java.lang.Object";
        if (ref instanceof ParameterizedTypeTree generic)
            ref = generic.getType();
        String name = ref.toString();
        if (name.contains(".") || name.equals("Object"))
            return name.equals("Object") ? "java.lang.Object" : name;
        for (ImportTree use : row.unit.getImports()) {
            String type = use.getQualifiedIdentifier().toString();
            if (!use.isStatic() && type.endsWith("." + name))
                return type;
        }
        return row.unit.getPackageName() + "." + name;
    }

    private static List<Column> columns(Map<String, Row> rows, String name) {
        List<Column> result = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        Set<String> names = new HashSet<>();
        Set<Integer> orders = new HashSet<>();
        boolean descriptor = false;
        while (!name.equals("java.lang.Object")) {
            Row row = rows.get(name);
            if (row == null || !visited.add(name))
                return List.of();
            descriptor |= row.type.getModifiers().getAnnotations().stream()
                .anyMatch(a -> named(row.unit, a.getAnnotationType(), GENERATOR + "SystemViewDescriptor"));
            for (Tree member : row.type.getMembers()) {
                if (!(member instanceof MethodTree method) || method.getReturnType() == null
                    || method.getReturnType().toString().equals("void"))
                    continue;
                Set<Modifier> flags = method.getModifiers().getFlags();
                if (!flags.contains(Modifier.PUBLIC) || flags.contains(Modifier.STATIC))
                    continue;
                String column = method.getName().toString();
                if (!method.getParameters().isEmpty() || !names.add(column))
                    return List.of();
                Integer order = null;
                for (AnnotationTree annotation : method.getModifiers().getAnnotations()) {
                    Tree annotationType = annotation.getAnnotationType();
                    if (!named(row.unit, annotationType, GENERATOR + "Order")) {
                        if (annotationType.toString().equals("Order"))
                            return List.of();
                        continue;
                    }
                    var args = annotation.getArguments();
                    if (args.isEmpty())
                        order = 0;
                    else if (args.size() == 1) {
                        var value = args.get(0);
                        if (value instanceof AssignmentTree assignment && assignment.getVariable().toString().equals("value"))
                            value = assignment.getExpression();
                        if (JavaContracts.literal(value) instanceof Integer rank)
                            order = rank;
                    }
                    // Ambiguous ranks rely on the order in which javac enumerates members.
                    if (order == null || !orders.add(order))
                        return List.of();
                }
                result.add(new Column(column, order, row.file));
            }
            name = parent(row);
        }
        result.sort(Comparator.comparing(Column::order, Comparator.nullsLast(Integer::compareTo)).thenComparing(Column::name));
        return descriptor ? result : List.of();
    }

    private static String viewName(ExpressionTree expr, ClassTree cls, MethodTree method, CompilationUnitTree unit, Map<String, Row> rows) {
        if (expr instanceof IdentifierTree id) {
            Set<String> locals = new HashSet<>();
            new TreeScanner<Void, Void>() {
                @Override public Void visitVariable(VariableTree var, Void unused) {
                    locals.add(var.getName().toString());
                    return super.visitVariable(var, unused);
                }
            }.scan(method, null);
            if (locals.contains(id.getName().toString()))
                return null;
            for (ImportTree use : unit.getImports()) {
                String imported = use.getQualifiedIdentifier().toString();
                if (use.isStatic() && imported.endsWith("." + id.getName())) {
                    Row row = rows.get(imported.substring(0, imported.lastIndexOf('.')));
                    if (row == null || cls.getMembers().stream().anyMatch(m ->
                        m instanceof VariableTree field && field.getName().equals(id.getName())))
                        return null;
                    cls = row.type;
                    unit = row.unit;
                    break;
                }
            }
            for (Tree member : cls.getMembers()) {
                if (member instanceof VariableTree field && id.getName().equals(field.getName())
                    && field.getModifiers().getFlags().containsAll(Set.of(Modifier.STATIC, Modifier.FINAL))) {
                    expr = field.getInitializer();
                    break;
                }
            }
        }
        if (expr instanceof LiteralTree literal && literal.getValue() instanceof String value)
            return value;
        if (expr instanceof MethodInvocationTree call && (named(unit, call.getMethodSelect(),
            "org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName")
            || call.getMethodSelect() instanceof MemberSelectTree selected && selected.getIdentifier().contentEquals("metricName")
                && named(unit, selected.getExpression(), "org.apache.ignite.internal.processors.metric.impl.MetricUtils"))) {
            List<String> parts = new ArrayList<>();
            for (ExpressionTree arg : call.getArguments()) {
                if (!(arg instanceof LiteralTree literal) || !(literal.getValue() instanceof String part))
                    return null;
                parts.add(part);
            }
            return parts.isEmpty() ? null : String.join(".", parts);
        }
        return null;
    }

    // The scan view passes its name and generated walker to AbstractSystemView's constructor.
    private static String scanViewName(Map<Path, CompilationUnitTree> units, Map<String, Row> rows) {
        Path wrapper = MANAGER_PATH.resolve("ScanQuerySystemView.java");
        CompilationUnitTree unit = units.get(wrapper);
        if (unit == null || !(unit.getPackageName() + ".").equals(MANAGERS) || unit.getTypeDecls().size() != 1
            || !(unit.getTypeDecls().get(0) instanceof ClassTree cls) || !cls.getSimpleName().contentEquals("ScanQuerySystemView")
            || !parent(new Row(wrapper, unit, cls)).equals(MANAGERS + "AbstractSystemView"))
            return null;
        String result = null;
        for (Tree member : cls.getMembers()) {
            if (!(member instanceof MethodTree method))
                continue;
            if (Set.of("name", "walker").contains(method.getName().toString()))
                return null;
            if (method.getReturnType() != null)
                continue;
            if (result != null || method.getBody() == null || method.getBody().getStatements().isEmpty()
                || !(method.getBody().getStatements().get(0) instanceof ExpressionStatementTree first)
                || !(first.getExpression() instanceof MethodInvocationTree call) || !call.getMethodSelect().toString().equals("super")
                || call.getArguments().size() != 3 || !(call.getArguments().get(2) instanceof NewClassTree walker)
                || walker.getClassBody() != null || !named(unit, walker.getIdentifier(), GENERATOR + "ScanQueryViewWalker"))
                return null;
            result = viewName(call.getArguments().get(0), cls, method, unit, rows);
            if (result == null)
                return null;
        }
        return result;
    }

    private static Map<String, Set<String>> registrations(Map<Path, CompilationUnitTree> units, Map<String, Row> rows) {
        Map<String, Set<String>> views = new HashMap<>();
        String scan = units.containsKey(MANAGER_PATH.resolve("AbstractSystemView.java")) ? null : scanViewName(units, rows);
        for (CompilationUnitTree unit : units.values()) {
            if (!unit.getImports().stream().anyMatch(i -> i.getQualifiedIdentifier().toString().startsWith(GENERATOR)
                && i.getQualifiedIdentifier().toString().endsWith("Walker"))
                && !(scan != null && imported(unit, MANAGERS + "ScanQuerySystemView")))
                continue;
            for (Tree decl : unit.getTypeDecls()) {
                if (!(decl instanceof ClassTree cls))
                    continue;
                for (Tree member : cls.getMembers()) {
                    if (!(member instanceof MethodTree method))
                        continue;
                    new TreeScanner<Void, Void>() {
                        @Override public Void visitMethodInvocation(MethodInvocationTree call, Void unused) {
                            var args = call.getArguments();
                            if (call.getMethodSelect() instanceof MemberSelectTree select
                                && REGISTRATIONS.contains(select.getIdentifier().toString()) && args.size() >= 3
                                && args.get(2) instanceof NewClassTree walker && walker.getClassBody() == null
                                && systemViewManager(select.getExpression(), method, unit)) {
                                String type = walker.getIdentifier().toString();
                                String shortName = type.substring(type.lastIndexOf('.') + 1);
                                String name = viewName(args.get(0), cls, method, unit, rows);
                                if (name != null && shortName.endsWith("Walker") && named(unit, walker.getIdentifier(), GENERATOR + shortName))
                                    views.computeIfAbsent(shortName, ignored -> new TreeSet<>()).add(name);
                            }
                            if (scan != null && args.size() == 1 && call.getMethodSelect() instanceof MemberSelectTree select
                                && select.getIdentifier().contentEquals("registerView") && systemViewManager(select.getExpression(), method, unit)
                                && args.get(0) instanceof NewClassTree view && view.getClassBody() == null) {
                                Tree type = view.getIdentifier() instanceof ParameterizedTypeTree generic ? generic.getType() : view.getIdentifier();
                                if (named(unit, type, MANAGERS + "ScanQuerySystemView"))
                                    views.computeIfAbsent("ScanQueryViewWalker", ignored -> new TreeSet<>()).add(scan);
                            }
                            return super.visitMethodInvocation(call, unused);
                        }
                    }.scan(method.getBody(), null);
                }
            }
        }
        return views;
    }

    private static boolean systemViewManager(ExpressionTree receiver, MethodTree method, CompilationUnitTree unit) {
        if (receiver instanceof MethodInvocationTree get && get.getArguments().isEmpty()
            && get.getMethodSelect() instanceof MemberSelectTree name && name.getIdentifier().contentEquals("systemView"))
            return true;
        return method.getParameters().stream().anyMatch(p -> p.getName().contentEquals(receiver.toString())
            && named(unit, p.getType(), "org.apache.ignite.internal.managers.systemview.GridSystemViewManager"));
    }

    private static Path changedPrefix(List<Column> prefix, List<Column> other) {
        for (Column col : prefix) {
            if (!other.contains(col))
                return col.file;
        }
        return null;
    }

    static void compare(Map<Path, CompilationUnitTree> before, Map<Path, CompilationUnitTree> after,
        BiConsumer<Path, String> emit) {
        if (before.keySet().stream().anyMatch(p -> p.startsWith(CODEGEN)) || after.keySet().stream().anyMatch(p -> p.startsWith(CODEGEN)))
            return;
        Map<String, Row> oldRows = rows(before);
        Map<String, Row> newRows = rows(after);
        if (oldRows.isEmpty() || newRows.isEmpty())
            return;
        var oldViews = registrations(before, oldRows);
        var newViews = registrations(after, newRows);
        for (String type : new TreeSet<>(newRows.keySet())) {
            Row row = newRows.get(type);
            String walker = row.type.getSimpleName() + "Walker";
            Set<String> views = new TreeSet<>(newViews.getOrDefault(walker, Set.of()));
            views.retainAll(oldViews.getOrDefault(walker, Set.of()));
            if (views.isEmpty() || row.type.getModifiers().getFlags().contains(Modifier.ABSTRACT))
                continue;
            List<Column> old = columns(oldRows, type);
            List<Column> now = columns(newRows, type);
            List<String> previousNames = old.stream().map(Column::name).toList();
            for (int i = 0; i < now.size(); i++) {
                String column = now.get(i).name;
                int previous = previousNames.indexOf(column);
                if (previous < 0 || previous == i)
                    continue;
                // A column moves if its rank or a preceding declaration changes, including in an ancestor.
                Path source = changedPrefix(now.subList(0, i + 1), old);
                if (source == null)
                    source = changedPrefix(old.subList(0, previous + 1), now);
                if (source != null) {
                    for (String view : views)
                        emit.accept(source, "Changed column order in system view " + view + ": " + column
                            + " moved from position " + (previous + 1) + " to " + (i + 1));
                }
                break;
            }
        }
    }
}
