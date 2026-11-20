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
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiConsumer;

/** Arguments of executable commands reached through the built-in control.sh registries. */
class SubcommandArguments {
    private static final String MANAGEMENT = "org.apache.ignite.internal.management.";
    private static final String API = MANAGEMENT + "api.";
    private static final String DTO = "org.apache.ignite.internal.dto.IgniteDataTransferObject";
    private static final Set<String> EXECUTABLE = Set.of(API + "ComputeCommand", API + "NativeCommand",
        API + "BeforeNodeStartCommand", API + "HelpCommand", API + "OfflineCommand");

    private record Source(Path path, String pkg, Map<String, String> imports, ClassTree cls) {
        String resolve(Tree type) {
            if (type instanceof ParameterizedTypeTree generic)
                return resolve(generic.getType());
            if (type == null)
                return "";
            String name = type.toString();
            return name.contains(".") ? name : imports.getOrDefault(name, pkg + "." + name);
        }

        boolean annotated(ModifiersTree mods, String name) {
            return mods.getAnnotations().stream().anyMatch(a -> resolve(a.getAnnotationType()).equals(API + name));
        }
    }

    private record Argument(Path path, String type, Boolean required, Boolean sensitive, boolean deprecated) {}
    private record Group(Boolean optional, Boolean exclusive) {}
    private record Binding(Source dto, Path method) {}
    private record Arguments(Path command, Path binding, Path dto, Map<String, Argument> named,
        List<Argument> positional, Map<List<String>, Group> groups) {
        Group rule(List<String> options) {
            Group group = groups.get(options);
            if (group != null)
                return group;
            // Separate optional options accept both an empty set and more than one value.
            return options.stream().allMatch(key -> named.containsKey(key) && Boolean.FALSE.equals(named.get(key).required))
                ? new Group(true, false) : null;
        }
    }

    static void compare(Map<Path, CompilationUnitTree> before, Map<Path, CompilationUnitTree> after,
        BiConsumer<Path, String> emit) {
        Map<String, Arguments> old = commands(before);
        commands(after).forEach((name, now) -> {
            Arguments was = old.get(name);
            if (was == null || !was.command.equals(now.command))
                return;
            // A command can select a different existing DTO without editing either DTO.
            boolean switched = !was.dto.equals(now.dto);
            BiConsumer<Path, String> report = (path, fact) -> {
                if (changed(path, before, after))
                    emit.accept(path, fact);
                else if (changed(now.dto, before, after))
                    emit.accept(now.dto, fact);
                else if (changed(now.binding, before, after))
                    emit.accept(now.binding, fact);
                else if (changed(now.command, before, after))
                    emit.accept(now.command, fact);
            };
            for (var entry : was.named.entrySet()) {
                if (!entry.getValue().deprecated && !now.named.containsKey(entry.getKey()))
                    report.accept(switched ? now.command : entry.getValue().path, "Removed CLI option " + name + " " + entry.getKey());
            }
            now.named.forEach((option, arg) -> {
                Path path = switched ? now.command : arg.path;
                String subject = "CLI option " + name + " " + option;
                Argument prior = was.named.get(option);
                if (prior == null)
                    report.accept(path, "Added " + subject);
                else {
                    compareArgument(prior, arg, subject, path, report);
                    if (option.startsWith("--") && "string".equals(prior.type) && "string".equals(arg.type)
                        && prior.sensitive != null && arg.sensitive != null && !arg.sensitive.equals(prior.sensitive))
                        report.accept(path, subject + (arg.sensitive ? " now prompts for a missing value on an interactive console"
                            : " now requires a value on the command line"));
                }
            });
            int oldCount = was.positional.size();
            int count = now.positional.size();
            if (count > oldCount || was.positional.stream().filter(a -> !a.deprecated).count() > count) {
                Map<Path, Integer> counts = new TreeMap<>();
                now.positional.forEach(arg -> counts.merge(arg.path, 1, Integer::sum));
                was.positional.forEach(arg -> counts.merge(arg.path, -1, Integer::sum));
                Path origin = counts.entrySet().stream().filter(e -> e.getValue() != 0 && changed(e.getKey(), before, after))
                    .map(Map.Entry::getKey).findFirst().orElse(now.dto);
                report.accept(switched ? now.command : origin,
                    "Changed positional argument count for " + name + " from " + oldCount + " to " + count);
            }
            else if (oldCount == 1 && count == 1)
                compareArgument(was.positional.get(0), now.positional.get(0), "CLI positional argument for " + name,
                    switched ? now.command : now.positional.get(0).path, report);
            Set<List<String>> allGroups = new LinkedHashSet<>(was.groups.keySet());
            allGroups.addAll(now.groups.keySet());
            for (List<String> options : allGroups) {
                Group oldGroup = was.rule(options);
                Group group = now.rule(options);
                if (group == null || oldGroup == null)
                    continue;
                Path origin = switched ? now.command : now.dto;
                String names = String.join(", ", options);
                if (group.optional != null && oldGroup.optional != null && !group.optional.equals(oldGroup.optional))
                    report.accept(origin, group.optional ? name + " now allows omitting " + names
                        : name + " now requires at least one of " + names);
                if (group.exclusive != null && oldGroup.exclusive != null && !group.exclusive.equals(oldGroup.exclusive))
                    report.accept(origin, group.exclusive ? name + " now accepts at most one of " + names
                        : name + " now allows combining " + names);
            }
        });
    }

    private static boolean changed(Path path, Map<Path, CompilationUnitTree> before, Map<Path, CompilationUnitTree> after) {
        return before.get(path) == null || after.get(path) == null || !before.get(path).toString().equals(after.get(path).toString());
    }

    private static void compareArgument(Argument old, Argument now, String subject, Path path, BiConsumer<Path, String> emit) {
        if (old.type != null && now.type != null && !old.type.equals(now.type))
            emit.accept(path, "Changed " + subject + " type from " + old.type + " to " + now.type);
        if (old.required != null && now.required != null && !old.required.equals(now.required))
            emit.accept(path, subject + " is now " + (now.required ? "required" : "optional"));
    }

    private static Map<String, Arguments> commands(Map<Path, CompilationUnitTree> units) {
        if (units.keySet().stream().noneMatch(path -> path.endsWith("management/IgniteCommandRegistry.java")))
            return Map.of();
        Map<String, Source> types = new HashMap<>();
        units.forEach((path, unit) -> {
            if (unit.getPackageName() == null)
                return;
            String pkg = unit.getPackageName().toString();
            Map<String, String> imports = new HashMap<>();
            for (ImportTree item : unit.getImports()) {
                String name = item.getQualifiedIdentifier().toString();
                // Without attribution a wildcard can refer to a different annotation, DTO or command.
                if (name.endsWith(".*") && !item.isStatic())
                    return;
                if (!item.isStatic())
                    imports.put(name.substring(name.lastIndexOf('.') + 1), name);
            }
            for (Tree member : unit.getTypeDecls()) {
                if (member instanceof ClassTree cls && path.getFileName().toString().equals(cls.getSimpleName() + ".java")) {
                    String name = pkg + "." + cls.getSimpleName();
                    for (Tree child : cls.getMembers()) {
                        if (child instanceof ClassTree nested) {
                            String nestedName = name + "." + nested.getSimpleName();
                            imports.put(nested.getSimpleName().toString(), nestedName);
                            types.put(nestedName, new Source(path, pkg, imports, nested));
                        }
                    }
                    types.put(name, new Source(path, pkg, imports, cls));
                }
            }
        });
        Map<String, Arguments> result = new TreeMap<>();
        Source root = types.get(MANAGEMENT + "IgniteCommandRegistry");
        if (root != null)
            visitRegistry(root, "", types, result, new HashSet<>());
        return result;
    }

    private static void visitRegistry(Source registry, String parent, Map<String, Source> types,
        Map<String, Arguments> result, Set<Path> visiting) {
        if (!registry.resolve(registry.cls.getExtendsClause()).equals(API + "CommandRegistryImpl") || !visiting.add(registry.path))
            return;
        MethodTree constructor = null;
        for (Tree member : registry.cls.getMembers()) {
            if (!(member instanceof MethodTree method))
                continue;
            if (method.getName().contentEquals("command"))
                return;
            if (method.getName().contentEquals("register") && (method.getBody() == null || method.getParameters().size() != 1
                || !method.getBody().getStatements().toString().equals("super.register(" + method.getParameters().get(0).getName() + ");")))
                return;
            if (method.getReturnType() == null && method.getParameters().isEmpty())
                constructor = method;
        }
        if (constructor == null || constructor.getBody() == null || constructor.getBody().getStatements().isEmpty()
            || !(constructor.getBody().getStatements().get(0) instanceof ExpressionStatementTree first)
            || !(first.getExpression() instanceof MethodInvocationTree registration) || !registration.getMethodSelect().toString().equals("super"))
            return;
        String prefix = parent.isEmpty() ? "" : stem(registry.cls.getSimpleName().toString());
        boolean dashes = parent.isEmpty() || registry.annotated(registry.cls.getModifiers(), "CliSubcommandsWithPrefix");
        for (ExpressionTree arg : registration.getArguments()) {
            if (!(arg instanceof NewClassTree init) || init.getClassBody() != null)
                continue;
            Source child = types.get(registry.resolve(init.getIdentifier()));
            if (child == null)
                continue;
            String key = stem(child.cls.getSimpleName().toString());
            if (key == null || prefix == null || !key.startsWith(prefix) || key.length() == prefix.length())
                continue;
            String word = formatted(key.substring(prefix.length()), dashes ? '-' : '_');
            String name = (parent.isEmpty() ? "" : parent + " ") + (dashes ? "--" : "") + word;
            if (!parent.isEmpty()) {
                Binding binding = argumentClass(child, types);
                if (binding != null) {
                    Arguments options = arguments(child, binding, types);
                    if (options != null)
                        result.put(name, options);
                }
            }
            visitRegistry(child, name, types, result, visiting);
        }
        visiting.remove(registry.path);
    }

    private static String stem(String name) {
        return name.matches("[A-Za-z][A-Za-z0-9]*Command") ? name.substring(0, name.length() - "Command".length()) : null;
    }

    private static Binding argumentClass(Source command, Map<String, Source> types) {
        boolean executable = false;
        Binding binding = null;
        Set<ClassTree> seen = new HashSet<>();
        for (Source source = command; source != null && seen.add(source.cls); source = types.get(source.resolve(source.cls.getExtendsClause()))) {
            executable |= source.cls.getImplementsClause().stream().map(source::resolve).anyMatch(EXECUTABLE::contains);
            for (Tree member : source.cls.getMembers()) {
                if (binding != null || !(member instanceof MethodTree method) || !method.getName().contentEquals("argClass")
                    || !method.getParameters().isEmpty())
                    continue;
                if (method.getBody() == null || method.getBody().getStatements().size() != 1
                    || !(method.getBody().getStatements().get(0) instanceof ReturnTree ret)
                    || !(ret.getExpression() instanceof MemberSelectTree type) || !type.getIdentifier().contentEquals("class"))
                    return null;
                Source dto = types.get(source.resolve(type.getExpression()));
                if (dto == null)
                    return null;
                binding = new Binding(dto, source.path);
            }
            if (executable && binding != null)
                return binding;
        }
        return null;
    }

    private static Arguments arguments(Source command, Binding binding, Map<String, Source> types) {
        Source dto = binding.dto;
        Set<String> grouped = new HashSet<>();
        Map<List<String>, Group> restrictions = new LinkedHashMap<>();
        for (AnnotationTree annotation : dto.cls.getModifiers().getAnnotations()) {
            String type = dto.resolve(annotation.getAnnotationType());
            if (type.equals(API + "ArgumentGroupsHolder"))
                return null;
            if (type.equals(API + "ArgumentGroup")) {
                ExpressionTree value = attribute(annotation, "value");
                if (value == null)
                    return null;
                List<? extends ExpressionTree> members = value instanceof NewArrayTree array ? array.getInitializers() : List.of(value);
                if (members == null)
                    return null;
                List<String> fields = new ArrayList<>();
                for (ExpressionTree entry : members) {
                    if (!(entry instanceof LiteralTree lit) || !(lit.getValue() instanceof String name) || !grouped.add(name))
                        return null;
                    fields.add(name);
                }
                restrictions.put(fields, new Group(flag(annotation, "optional"), flag(annotation, "onlyOneOf")));
            }
        }
        Map<String, Argument> named = new TreeMap<>();
        Map<String, String> externalNames = new HashMap<>();
        List<Argument> positional = new ArrayList<>();
        Set<ClassTree> seen = new HashSet<>();
        for (Source source = dto; source != null && seen.add(source.cls); source = types.get(source.resolve(source.cls.getExtendsClause()))) {
            for (Tree member : source.cls.getMembers()) {
                if (!(member instanceof VariableTree field))
                    continue;
                for (AnnotationTree arg : field.getModifiers().getAnnotations()) {
                    if (!source.resolve(arg.getAnnotationType()).equals(API + "Argument"))
                        continue;
                    String id = field.getName().toString();
                    Boolean optional = flag(arg, "optional");
                    Boolean required = optional == null || grouped.contains(id) ? null : !optional;
                    ExpressionTree description = attribute(arg, "description");
                    String help = description instanceof LiteralTree lit && lit.getValue() instanceof String text
                        ? text.strip().toLowerCase(Locale.ROOT) : "";
                    boolean deprecated = field.getModifiers().getAnnotations().stream().anyMatch(a ->
                        Set.of("Deprecated", "java.lang.Deprecated").contains(a.getAnnotationType().toString()))
                        || help.startsWith("deprecated.")
                        || id.equals("nodeId") && help.endsWith("(deprecated. use --node-ids or --all-nodes instead)");
                    Argument option = new Argument(source.path, inputType(field.getType(), source), required, flag(arg, "sensitive"), deprecated);
                    if (source.annotated(field.getModifiers(), "Positional"))
                        positional.add(option);
                    else {
                        Boolean noPrefix = flag(arg, "withoutPrefix");
                        Boolean javaStyle = flag(arg, "javaStyleName");
                        if (noPrefix == null || javaStyle == null)
                            return null;
                        String name = (noPrefix ? "" : "--") + (javaStyle ? id.toLowerCase(Locale.ROOT) : formatted(id, '-'));
                        externalNames.put(id, name);
                        if (named.put(name, option) != null)
                            return null;
                    }
                }
            }
            if (source.resolve(source.cls.getExtendsClause()).equals(DTO)) {
                Map<List<String>, Group> groups = new LinkedHashMap<>();
                restrictions.forEach((ids, rule) -> {
                    if (ids.size() >= 2 && externalNames.keySet().containsAll(ids)) {
                        List<String> options = ids.stream().map(externalNames::get).sorted().toList();
                        groups.put(options, rule);
                    }
                });
                return new Arguments(command.path, binding.method, dto.path, named, positional, groups);
            }
        }
        return null;
    }

    private static String formatted(String id, char delimiter) {
        StringBuilder name = new StringBuilder();
        for (int i = 0; i < id.length(); i++) {
            if (i > 0 && !Character.isLowerCase(id.charAt(i)))
                name.append(delimiter);
            name.append(Character.toLowerCase(id.charAt(i)));
        }
        return name.toString();
    }

    private static ExpressionTree attribute(AnnotationTree annotation, String name) {
        for (ExpressionTree arg : annotation.getArguments()) {
            if (arg instanceof AssignmentTree assignment && assignment.getVariable().toString().equals(name))
                return assignment.getExpression();
        }
        return null;
    }

    private static Boolean flag(AnnotationTree annotation, String name) {
        ExpressionTree expr = attribute(annotation, name);
        if (expr == null)
            return false;
        return expr instanceof LiteralTree lit && lit.getValue() instanceof Boolean b ? b : null;
    }

    private static String inputType(Tree type, Source source) {
        if (type instanceof ArrayTypeTree array) {
            if (array.getType().toString().equals("char"))
                return "string";
            String element = inputType(array.getType(), source);
            return element == null ? null : element + " list";
        }
        String raw = type.toString();
        String name = source.imports.getOrDefault(raw, raw);
        return switch (name) {
            case "boolean", "Boolean", "java.lang.Boolean" -> "boolean";
            case "int", "Integer", "java.lang.Integer" -> "int";
            case "long", "Long", "java.lang.Long" -> "long";
            case "float", "Float", "java.lang.Float" -> "float";
            case "double", "Double", "java.lang.Double" -> "double";
            case "String", "java.lang.String" -> "string";
            case "java.util.UUID" -> "UUID";
            case "org.apache.ignite.lang.IgniteUuid" -> "IgniteUuid";
            default -> null;
        };
    }
}
