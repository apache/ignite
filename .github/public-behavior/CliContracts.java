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
import java.util.function.Consumer;
import javax.lang.model.element.Modifier;

class CliContracts {
    static void compare(Path file, ClassTree before, ClassTree after, Consumer<String> output) {
        // Moving declarations to another class may preserve the CLI.
        if (before == null || after == null)
            return;

        String name = file.getFileName().toString();
        if (name.equals("CommandHandler.java")) {
            compareExitCodes(before, after, output);
            return;
        }

        boolean commands = name.equals("IgniteCommandRegistry.java");
        if (!commands && !name.equals("ArgumentParser.java"))
            return;
        Registrations old = registrations(before, commands);
        Registrations now = registrations(after, commands);
        if (old == null || now == null || !old.otherStatements().equals(now.otherStatements()))
            return;
        String label = commands ? "CLI command " : "CLI option ";
        old.options().forEach((nameBefore, type) -> {
            if (!now.options().containsKey(nameBefore))
                output.accept("Removed " + label + nameBefore);
        });
        now.options().forEach((nameAfter, type) -> {
            if (!old.options().containsKey(nameAfter))
                output.accept("Added " + label + nameAfter);
            else if (type != null && old.options().get(nameAfter) != null && !type.equals(old.options().get(nameAfter)))
                output.accept("Changed CLI option " + nameAfter + " type from " + old.options().get(nameAfter) + " to " + type);
        });
    }

    private record Registration(String name, String type) {}
    private record Registrations(Map<String, String> options, List<String> otherStatements) {}

    private static Registrations registrations(ClassTree cls, boolean commands) {
        List<MethodTree> constructors = cls.getMembers().stream().filter(MethodTree.class::isInstance)
            .map(MethodTree.class::cast).filter(m -> m.getReturnType() == null).toList();
        if (constructors.size() != 1 || constructors.get(0).getBody() == null)
            return null;
        Map<String, Object> constants = constants(cls);
        Map<String, String> options = new TreeMap<>();
        List<String> other = new ArrayList<>();
        boolean found = false;
        for (StatementTree stmt : constructors.get(0).getBody().getStatements()) {
            List<? extends ExpressionTree> args = null;
            if (stmt instanceof ExpressionStatementTree expr && expr.getExpression() instanceof MethodInvocationTree call) {
                if (commands && call.getMethodSelect().toString().equals("super"))
                    args = call.getArguments();
                else if (!commands && call.getMethodSelect().toString().equals("common.addAll")
                    && call.getArguments().size() == 1 && call.getArguments().get(0) instanceof MethodInvocationTree list
                    && Set.of("List.of", "java.util.List.of").contains(list.getMethodSelect().toString()))
                    args = list.getArguments();
            }
            if (args == null) {
                // A helper can register the same names that were removed from the list.
                other.add(stmt.toString());
                continue;
            }
            if (found)
                return null;
            found = true;
            for (ExpressionTree arg : args) {
                Registration registration = commands ? command(arg) : option(arg, constants);
                if (registration == null)
                    return null;
                options.put(registration.name(), registration.type());
            }
        }
        return found ? new Registrations(options, other) : null;
    }

    private static Registration command(ExpressionTree arg) {
        if (!(arg instanceof NewClassTree cmd) || cmd.getClassBody() != null)
            return null;
        String name = cmd.getIdentifier().toString();
        name = name.substring(name.lastIndexOf('.') + 1);
        if (!name.matches("[A-Z][a-zA-Z]*Command"))
            return null;
        // CommandUtils inserts a dash before each uppercase letter after the first one.
        String stem = name.substring(0, name.length() - "Command".length());
        return new Registration("--" + stem.replaceAll("(?<!^)([A-Z])", "-$1").toLowerCase(Locale.ROOT), null);
    }

    private static Registration option(ExpressionTree expr, Map<String, Object> constants) {
        while (expr instanceof MethodInvocationTree call) {
            String method = call.getMethodSelect().toString();
            if (Set.of("argument", "optionalArgument").contains(method) && call.getArguments().size() == 2) {
                ExpressionTree arg = call.getArguments().get(0);
                Object name = arg instanceof LiteralTree lit ? lit.getValue() : constants.get(arg.toString());
                if (!(name instanceof String option) || !option.matches("--[a-z0-9-]+"))
                    return null;
                return new Registration(option, optionType(call.getArguments().get(1)));
            }
            if (!(call.getMethodSelect() instanceof MemberSelectTree chain))
                break;
            expr = chain.getExpression();
        }
        return null;
    }

    private static String optionType(ExpressionTree type) {
        if (!(type instanceof MemberSelectTree selected) || !selected.getIdentifier().contentEquals("class"))
            return null;
        // CommandUtils parses primitive and boxed options alike. A password is read as one string.
        String name = selected.getExpression().toString();
        if (name.startsWith("java.lang."))
            name = name.substring("java.lang.".length());
        return switch (name) {
            case "Boolean", "boolean" -> "boolean";
            case "Integer", "int" -> "int";
            case "Long", "long" -> "long";
            case "Float", "float" -> "float";
            case "Double", "double" -> "double";
            case "String", "char[]" -> "string";
            case "String[]" -> "string list";
            default -> null;
        };
    }

    private static Map<String, Object> constants(ClassTree cls) {
        Map<String, Object> values = new HashMap<>();
        for (Tree member : cls.getMembers()) {
            if (member instanceof VariableTree field && field.getInitializer() instanceof LiteralTree literal
                && field.getModifiers().getFlags().containsAll(Set.of(Modifier.STATIC, Modifier.FINAL)))
                values.put(field.getName().toString(), literal.getValue());
        }
        return values;
    }

    private static MethodTree execute(ClassTree cls) {
        for (Tree member : cls.getMembers()) {
            if (member instanceof MethodTree method && method.getName().contentEquals("execute")
                && "int".equals(String.valueOf(method.getReturnType())) && method.getParameters().size() == 1
                && Set.of("List<String>", "java.util.List<String>").contains(method.getParameters().get(0).getType().toString()))
                return method;
        }
        return null;
    }

    private static void compareExitCodes(ClassTree before, ClassTree after, Consumer<String> output) {
        MethodTree old = execute(before);
        MethodTree now = execute(after);
        // If the dispatch changed, a different return statement may still give callers the same code.
        if (old == null || now == null || old.getBody() == null || now.getBody() == null
            || !old.getBody().toString().equals(now.getBody().toString()))
            return;
        Set<String> returned = new TreeSet<>();
        Set<String> locals = new HashSet<>();
        new TreeScanner<Void, Void>() {
            @Override public Void visitVariable(VariableTree var, Void p) {
                locals.add(var.getName().toString());
                return super.visitVariable(var, p);
            }

            @Override public Void visitReturn(ReturnTree ret, Void p) {
                if (ret.getExpression() instanceof IdentifierTree id)
                    returned.add(id.getName().toString());
                return null;
            }

            @Override public Void visitLambdaExpression(LambdaExpressionTree lambda, Void p) { return null; }
            @Override public Void visitClass(ClassTree nested, Void p) { return null; }
        }.scan(old.getBody(), null);
        returned.removeAll(locals);
        Map<String, Object> oldValues = constants(before);
        Map<String, Object> newValues = constants(after);
        for (String status : returned) {
            if (oldValues.get(status) instanceof Integer from && newValues.get(status) instanceof Integer to
                && !from.equals(to))
                output.accept("Changed CLI exit code for " + status + " from " + from + " to " + to);
        }
    }
}
