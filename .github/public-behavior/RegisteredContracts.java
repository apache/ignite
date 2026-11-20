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
import java.util.*;
import java.util.function.Consumer;
import javax.lang.model.element.Modifier;

/** Compare registrations whose public values are explicit in both revisions. */
class RegisteredContracts extends TreeScanner<Void, Void> {
    private record Slot(String kind, String binding) {}
    private final Map<Slot, String> values = new HashMap<>();
    private final Map<String, VariableTree> fields = new HashMap<>();
    private Set<String> shadowed = new HashSet<>();
    private int registrationCalls;
    private boolean allSettingsKnown = true;
    private final Map<String, String> defaults = new HashMap<>();
    private Map<String, String> variableTypes = new HashMap<>();
    private static final Set<String> PROPERTY_FACTORIES = Set.of("detachedBooleanProperty",
        "detachedLongProperty", "detachedIntegerProperty", "detachedDoubleProperty");
    private static final Set<String> METRIC_FACTORIES = Set.of("longMetric", "longAdderMetric", "intMetric",
        "doubleMetric", "booleanMetric", "objectMetric", "hitRateMetric", "histogram");

    static void compare(ClassTree before, ClassTree after, Consumer<String> output) {
        if (before == null || after == null)
            return;
        if (before.getSimpleName().contentEquals("SqlKeyword")) {
            compareKeywords(before, after, output);
            return;
        }
        var old = new RegisteredContracts(before);
        var now = new RegisteredContracts(after);
        Set<String> messages = new TreeSet<>();
        old.values.forEach((slot, from) -> {
            String to = now.values.get(slot);
            if (from != null && to != null && !from.equals(to)
                && !(old.contains(slot.kind(), to) && now.contains(slot.kind(), from)))
                messages.add("Changed " + slot.kind() + " from " + from + " to " + to);
        });
        // A private property may already exist when someone exposes it through the dispatcher.
        // Require the same dispatcher calls and the same field definition on both sides.
        if (old.allSettingsKnown && now.allSettingsKnown && old.registrationCalls > 0
            && old.registrationCalls == now.registrationCalls) {
            for (String field : old.fields.keySet()) {
                String key = old.propertyKey(field);
                if (key == null || !key.equals(now.propertyKey(field)))
                    continue;
                boolean wasRegistered = old.contains("distributed setting name", key);
                boolean isRegistered = now.contains("distributed setting name", key);
                if (isRegistered != wasRegistered)
                    messages.add((isRegistered ? "Added" : "Removed") + " distributed setting registration for " + key);
            }
        }
        messages.forEach(output);
    }

    private record Keywords(Set<String> names, List<String> loader) {}

    private static void compareKeywords(ClassTree before, ClassTree after, Consumer<String> output) {
        Keywords old = keywords(before);
        Keywords now = keywords(after);
        if (old == null || now == null || !old.loader.equals(now.loader))
            return;
        old.names.stream().filter(name -> !now.names.contains(name))
            .forEach(name -> output.accept("Removed Ignite SQL parser keyword " + name));
        now.names.stream().filter(name -> !old.names.contains(name))
            .forEach(name -> output.accept("Added Ignite SQL parser keyword " + name));
    }

    private static Keywords keywords(ClassTree cls) {
        Set<String> names = new TreeSet<>();
        List<String> loader = new ArrayList<>();
        boolean readsStringFields = false;
        boolean checksMembership = false;
        for (Tree member : cls.getMembers()) {
            if (member instanceof VariableTree field && Set.of("String", "java.lang.String").contains(field.getType().toString())) {
                if (!field.getModifiers().getFlags().containsAll(Set.of(Modifier.STATIC, Modifier.FINAL))
                    || !(field.getInitializer() instanceof LiteralTree literal) || !(literal.getValue() instanceof String value))
                    return null;
                // Unquoted identifiers in the native SQL parser use these uppercase spellings.
                if (value.matches("[A-Z_][A-Z0-9_]*"))
                    names.add(value);
            }
            else {
                loader.add(member.toString());
                if (member instanceof BlockTree block && block.isStatic() && block.getStatements().size() == 2
                    && block.getStatements().get(0).toString().equals("KEYWORDS = new HashSet<>();")
                    && block.getStatements().get(1) instanceof TryTree attempt && attempt.getFinallyBlock() == null
                    && attempt.getBlock().getStatements().size() == 1
                    && attempt.getBlock().getStatements().get(0) instanceof EnhancedForLoopTree loop) {
                    readsStringFields = collectsKeywordValues(loop);
                }
                if (member instanceof MethodTree method && method.getName().contentEquals("isKeyword")
                    && method.getParameters().size() == 1 && method.getBody() != null
                    && method.getBody().getStatements().size() == 1
                    && method.getBody().getStatements().get(0) instanceof ReturnTree ret)
                    checksMembership = ("KEYWORDS.contains(" + method.getParameters().get(0).getName() + ")")
                        .equals(ret.getExpression().toString());
            }
        }
        return readsStringFields && checksMembership ? new Keywords(names, loader) : null;
    }

    private static boolean collectsKeywordValues(EnhancedForLoopTree loop) {
        if (!loop.getExpression().toString().equals("SqlKeyword.class.getDeclaredFields()")
            || !(loop.getStatement() instanceof BlockTree body) || body.getStatements().size() != 1
            || !(body.getStatements().get(0) instanceof IfTree guard) || guard.getElseStatement() != null
            || !(guard.getThenStatement() instanceof BlockTree selected) || selected.getStatements().size() != 2
            || !(selected.getStatements().get(0) instanceof VariableTree value) || value.getInitializer() == null)
            return false;
        String field = loop.getVariable().getName().toString();
        return guard.getCondition().toString().equals("(Objects.equals(String.class, " + field + ".getType()))")
            && value.getInitializer().toString().equals("(String)" + field + ".get(null)")
            && selected.getStatements().get(1).toString().equals("KEYWORDS.add(" + value.getName() + ");");
    }

    private boolean contains(String kind, String value) {
        return values.entrySet().stream().anyMatch(e -> e.getKey().kind().equals(kind) && value.equals(e.getValue()));
    }

    private void record(String kind, String binding, String value) {
        if (value == null)
            return;
        Slot slot = new Slot(kind, binding);
        if (values.containsKey(slot) && !value.equals(values.get(slot)))
            values.put(slot, null);
        else
            values.put(slot, value);
    }

    private RegisteredContracts(ClassTree cls) {
        for (Tree member : cls.getMembers()) {
            if (member instanceof VariableTree field) {
                fields.put(field.getName().toString(), field);
                if (field.getType() != null)
                    variableTypes.put(field.getName().toString(), field.getType().toString());
            }
        }
        if (cls.getSimpleName().contentEquals("IgniteOwnSqlOperatorTable")) {
            for (VariableTree field : fields.values()) {
                if (field.getModifiers().getFlags().containsAll(Set.of(Modifier.PUBLIC, Modifier.STATIC))
                    && field.getInitializer() instanceof NewClassTree init && init.getIdentifier().toString().equals("SqlFunction")
                    && !init.getArguments().isEmpty()) {
                    String name = text(init.getArguments().get(0));
                    if (name != null && !name.startsWith("$"))
                        record("Calcite SQL function name", field.getName().toString(), name.toUpperCase(Locale.ROOT));
                }
            }
        }
        scan(cls, null);
        defaults.forEach((field, value) -> {
            String key = values.get(new Slot("distributed setting name", field));
            if (key != null && !value.isEmpty())
                record("default for distributed setting " + key, key, value);
        });
    }

    private String text(ExpressionTree expr) {
        if (expr instanceof IdentifierTree id) {
            VariableTree field = fields.get(id.getName().toString());
            if (field == null || shadowed.contains(id.getName().toString())
                || !field.getModifiers().getFlags().containsAll(Set.of(Modifier.FINAL, Modifier.STATIC)))
                return null;
            expr = field.getInitializer();
        }
        return expr instanceof LiteralTree lit && lit.getValue() instanceof String s ? s : null;
    }

    private String propertyKey(String id) {
        VariableTree field = fields.get(id);
        if (field == null || !field.getModifiers().getFlags().contains(Modifier.PRIVATE)
            || !(field.getInitializer() instanceof MethodInvocationTree factory)
            || !PROPERTY_FACTORIES.contains(method(factory)) || factory.getArguments().isEmpty())
            return null;
        return text(factory.getArguments().get(0));
    }

    private static String method(MethodInvocationTree call) {
        return call.getMethodSelect() instanceof MemberSelectTree member
            ? member.getIdentifier().toString() : call.getMethodSelect().toString();
    }

    @Override public Void visitMethodInvocation(MethodInvocationTree call, Void unused) {
        String name = method(call);
        var args = call.getArguments();
        if (call.getMethodSelect() instanceof MemberSelectTree member) {
            String receiver = member.getExpression().toString();
            if (Set.of("registerProperty", "registerProperties").contains(name)
                && "DistributedPropertyDispatcher".equals(variableTypes.get(receiver))) {
                registrationCalls++;
                for (ExpressionTree arg : args) {
                    String id = arg.toString();
                    String key = shadowed.contains(id) ? null : propertyKey(id);
                    if (key == null) {
                        allSettingsKnown = false;
                        continue;
                    }
                    record("distributed setting name", id, key);
                    var factory = (MethodInvocationTree)fields.get(id).getInitializer();
                    String type = method(factory).replace("detached", "").replace("Property", "");
                    record("type of distributed setting " + key, key, type);
                }
            }
            else if (member.getExpression() instanceof MethodInvocationTree registry && method(registry).equals("systemView")
                && Set.of("registerView", "registerFiltrableView", "registerInnerCollectionView").contains(name)
                && args.size() >= 3 && args.get(2) instanceof NewClassTree walker) {
                record("system view name", walker.getIdentifier().toString(), text(args.get(0)));
            }
        }
        if (name.equals("setDefaultValue") && args.size() == 3 && args.get(0) instanceof IdentifierTree id
            && args.get(1) instanceof LiteralTree literal && literal.getValue() != null) {
            String value = literal.getValue().toString();
            String field = id.getName().toString();
            defaults.merge(field, value, (a, b) -> a.equals(b) ? a : "");
        }
        return super.visitMethodInvocation(call, unused);
    }

    @Override public Void visitAssignment(AssignmentTree assignment, Void unused) {
        String field = assignment.getVariable().toString();
        if (field.startsWith("this."))
            field = field.substring(5);
        else if (shadowed.contains(field))
            return super.visitAssignment(assignment, unused);
        if (fields.containsKey(field) && assignment.getExpression() instanceof MethodInvocationTree call
            && METRIC_FACTORIES.contains(method(call)) && call.getMethodSelect() instanceof MemberSelectTree member
            && "MetricRegistryImpl".equals(variableTypes.get(member.getExpression().toString()))
            && !call.getArguments().isEmpty())
            record("metric name", field, text(call.getArguments().get(0)));
        return super.visitAssignment(assignment, unused);
    }

    @Override public Void visitMethod(MethodTree method, Void unused) {
        var outerTypes = variableTypes;
        var outerShadowed = shadowed;
        variableTypes = new HashMap<>(outerTypes);
        shadowed = new HashSet<>(outerShadowed);
        super.visitMethod(method, unused);
        variableTypes = outerTypes;
        shadowed = outerShadowed;
        return null;
    }

    @Override public Void visitVariable(VariableTree var, Void unused) {
        String id = var.getName().toString();
        if (var.getType() != null && fields.get(id) != var) {
            variableTypes.put(id, var.getType().toString());
            if (fields.containsKey(id))
                shadowed.add(id);
        }
        if (var.getType() != null && var.getType().toString().equals("MetricRegistryImpl")
            && var.getInitializer() instanceof MethodInvocationTree call && call.getMethodSelect() instanceof MemberSelectTree member
            && "GridMetricManager".equals(variableTypes.get(member.getExpression().toString()))
            && method(call).equals("registry") && call.getArguments().size() == 1)
            record("metric registry name", var.getName().toString(), text(call.getArguments().get(0)));
        return super.visitVariable(var, unused);
    }
}
