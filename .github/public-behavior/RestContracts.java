/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import javax.lang.model.element.Modifier;

/** Compares the cmd values accepted by the HTTP REST handler. */
class RestContracts {
    private static final Path COMMANDS = Path.of("modules/core/src/main/java/org/apache/ignite/internal/"
        + "processors/rest/GridRestCommand.java");
    private static final Path HANDLER = Path.of("modules/rest-http/src/main/java/org/apache/ignite/internal/"
        + "processors/rest/protocols/http/jetty/GridJettyRestHandler.java");
    private static final Path PROTOCOL = HANDLER.resolveSibling("GridJettyRestProtocol.java");

    private static String mapping(ExpressionTree expr, String commandPath) {
        if (expr instanceof LiteralTree literal && literal.getValue() instanceof String value)
            return value;
        if (Set.of("IGNITE_CMD_PATH", "GridJettyRestHandler.IGNITE_CMD_PATH").contains(expr.toString()))
            return commandPath;
        if (expr instanceof BinaryTree concat && concat.getKind() == Tree.Kind.PLUS) {
            String prefix = mapping(concat.getLeftOperand(), commandPath);
            String suffix = mapping(concat.getRightOperand(), commandPath);
            return prefix == null || suffix == null ? null : prefix + suffix;
        }
        return null;
    }

    private record Endpoint(String path, String context, String registration) {}

    private static Endpoint endpoint(Map<Path, ClassTree> files) {
        ClassTree protocol = files.get(PROTOCOL);
        ClassTree handler = files.get(HANDLER);
        if (protocol == null || handler == null)
            return null;
        // An unqualified reference can otherwise resolve to a field on the protocol itself.
        if (protocol.getMembers().stream().anyMatch(member ->
            member instanceof VariableTree field && field.getName().contentEquals("IGNITE_CMD_PATH")))
            return null;
        boolean hasRestHandler = protocol.getMembers().stream().anyMatch(member ->
            member instanceof VariableTree field && field.getName().contentEquals("jettyHnd")
                && field.getType().toString().equals("GridJettyRestHandler"));
        if (!hasRestHandler)
            return null;
        String commandPath = null;
        for (Tree member : handler.getMembers()) {
            if (member instanceof VariableTree field && field.getName().contentEquals("IGNITE_CMD_PATH")
                && field.getModifiers().getFlags().containsAll(Set.of(Modifier.STATIC, Modifier.FINAL))
                && field.getInitializer() instanceof LiteralTree literal && literal.getValue() instanceof String value)
                commandPath = value;
        }
        for (Tree member : protocol.getMembers()) {
            if (!(member instanceof MethodTree method) || !method.getName().contentEquals("loadJettyConfiguration")
                || method.getBody() == null)
                continue;
            String context = null;
            String path = null;
            String registration = null;
            boolean registered = false;
            boolean mainContext = false;
            for (StatementTree stmt : method.getBody().getStatements()) {
                if (stmt instanceof VariableTree var && var.getName().contentEquals("IGNITE_CMD_PATH"))
                    return null;
                if (stmt instanceof VariableTree variable && variable.getName().contentEquals("mainCtx")
                    && variable.getInitializer() instanceof NewClassTree init
                    && init.getIdentifier().toString().equals("ServletContextHandler")) {
                    mainContext = true;
                    continue;
                }
                if (!(stmt instanceof ExpressionStatementTree expr) || !(expr.getExpression() instanceof MethodInvocationTree call)) {
                    if (stmt.toString().contains("mainCtx"))
                        return null;
                    continue;
                }
                var args = call.getArguments();
                switch (call.getMethodSelect().toString()) {
                    case "mainCtx.setContextPath" -> {
                        if (context != null || args.size() != 1 || !(args.get(0) instanceof LiteralTree literal)
                            || !(literal.getValue() instanceof String value))
                            return null;
                        context = value;
                    }
                    case "mainCtx.addServlet" -> {
                        if (args.size() != 2 || !(args.get(0) instanceof NewClassTree holder)
                            || !holder.getIdentifier().toString().equals("ServletHolder") || holder.getArguments().size() != 1)
                            return null;
                        var servlet = holder.getArguments().get(0);
                        if (servlet instanceof NewClassTree other && other.getIdentifier().toString().equals("WelcomeHandler"))
                            continue;
                        if (path != null || !servlet.toString().equals("jettyHnd"))
                            return null;
                        path = mapping(args.get(1), commandPath);
                        if (path == null)
                            return null;
                        registration = args.get(1).toString();
                    }
                    case "hnds.addHandler" -> registered |= args.size() == 1 && args.get(0).toString().equals("mainCtx");
                    default -> {
                        if (call.toString().contains("mainCtx"))
                            return null;
                    }
                }
            }
            if (mainContext && registered && context != null && context.startsWith("/") && path != null && path.startsWith("/"))
                return new Endpoint((context.equals("/") ? "" : context) + path, context, registration);
        }
        return null;
    }

    private static Set<String> caseNames(SwitchTree dispatch) {
        Set<String> commands = new HashSet<>();
        Set<String> pending = new HashSet<>();
        boolean rejectsUnknown = false;
        for (CaseTree branch : dispatch.getCases()) {
            List<? extends StatementTree> body = branch.getStatements();
            if (body == null)
                return null;
            if (body.size() == 1 && body.get(0) instanceof BlockTree block)
                body = block.getStatements();
            if (branch.getExpressions().isEmpty()) {
                rejectsUnknown = body.size() == 1 && body.get(0) instanceof ThrowTree;
                pending.clear();
                continue;
            }
            for (ExpressionTree label : branch.getExpressions()) {
                if (!(label instanceof IdentifierTree command))
                    return null;
                pending.add(command.getName().toString());
            }
            // Several cmd values can share a handler. An empty case takes the next case's body.
            if (body.isEmpty())
                continue;
            StatementTree last = body.get(body.size() - 1);
            if (last instanceof ReturnTree ret && (ret.getExpression() == null
                || ret.getExpression().getKind() == Tree.Kind.NULL_LITERAL))
                return null;
            if (last instanceof BreakTree || last instanceof ReturnTree)
                commands.addAll(pending);
            else if (!(last instanceof ThrowTree))
                return null;
            pending.clear();
        }
        return rejectsUnknown ? commands : null;
    }

    // Some GridRestCommand values have no HTTP handler. Read the switch that builds REST requests.
    private static Set<String> handledCommands(ClassTree handler) {
        for (Tree member : handler.getMembers()) {
            if (!(member instanceof MethodTree method) || !method.getName().contentEquals("createRequest")
                || method.getBody() == null)
                continue;
            for (StatementTree statement : method.getBody().getStatements()) {
                if (statement instanceof VariableTree var && var.getInitializer() == null)
                    continue;
                if (!(statement instanceof SwitchTree dispatch))
                    return null;
                ExpressionTree selector = dispatch.getExpression();
                while (selector instanceof ParenthesizedTree parens)
                    selector = parens.getExpression();
                if (!(selector instanceof IdentifierTree id) || method.getParameters().stream().noneMatch(param ->
                    param.getName().contentEquals(id.getName()) && param.getType().toString().equals("GridRestCommand")))
                    return null;
                return caseNames(dispatch);
            }
        }
        return null;
    }

    private record Snapshot(Set<String> declared, Map<String, Boolean> handled, List<String> lookup) {}

    private static Snapshot read(Map<Path, ClassTree> files) {
        ClassTree commands = files.get(COMMANDS);
        ClassTree handler = files.get(HANDLER);
        if (commands == null || handler == null || commands.getKind() != Tree.Kind.ENUM)
            return null;
        Set<String> cases = handledCommands(handler);
        if (cases == null)
            return null;
        Set<String> declared = new HashSet<>();
        Map<String, Boolean> handled = new TreeMap<>();
        List<String> lookup = new ArrayList<>();
        for (Tree member : commands.getMembers()) {
            if (!(member instanceof VariableTree var) || !(var.getInitializer() instanceof NewClassTree init)
                || !var.getType().toString().equals(commands.getSimpleName().toString())) {
                lookup.add(member.toString());
                continue;
            }
            if (init.getArguments().size() != 1 || !(init.getArguments().get(0) instanceof LiteralTree literal)
                || !(literal.getValue() instanceof String key))
                return null;
            if (!declared.add(key))
                return null;
            if (cases.remove(var.getName().toString())) {
                boolean deprecated = var.getModifiers().getAnnotations().stream().anyMatch(annotation ->
                    Set.of("Deprecated", "java.lang.Deprecated").contains(annotation.getAnnotationType().toString()));
                // The HTTP lookup converts the cmd parameter to lowercase before looking up the enum key.
                if (key.equals(key.toLowerCase(Locale.ROOT)))
                    handled.put(key, deprecated);
            }
        }
        for (Tree member : handler.getMembers()) {
            if (member instanceof MethodTree method && method.getName().contentEquals("command"))
                lookup.add(method.toString());
        }
        return cases.isEmpty() ? new Snapshot(declared, handled, lookup) : null;
    }

    static void compare(Map<Path, ClassTree> before, Map<Path, ClassTree> after, BiConsumer<Path, String> emit) {
        Endpoint previous = endpoint(before);
        Endpoint current = endpoint(after);
        if (previous != null && current != null && !previous.path.equals(current.path)) {
            boolean sameRegistration = previous.registration.equals(current.registration) && previous.context.equals(current.context);
            emit.accept(sameRegistration ? HANDLER : PROTOCOL,
                "Changed REST endpoint mapping from " + previous.path + " to " + current.path);
        }
        Snapshot old = read(before);
        Snapshot now = read(after);
        // A change in the lookup can introduce an alias that keeps the old cmd working.
        if (old == null || now == null || !old.lookup.equals(now.lookup))
            return;
        old.handled.forEach((key, deprecated) -> {
            if (!deprecated && !now.handled.containsKey(key))
                emit.accept(now.declared.contains(key) ? HANDLER : COMMANDS, "Removed REST command cmd=" + key);
        });
        for (String key : now.handled.keySet()) {
            if (!old.handled.containsKey(key))
                emit.accept(old.declared.contains(key) ? HANDLER : COMMANDS, "Added REST command cmd=" + key);
        }
    }
}
