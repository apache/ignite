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
import javax.lang.model.element.Modifier;

/** SQLState returned for a numeric Ignite error by the JDBC error mapping. */
class ErrorMapping {
    private static final Path ROOT = Path.of("modules/core/src/main/java/org/apache/ignite/internal/processors");
    private static final Path ERRORS = ROOT.resolve("cache/query/IgniteQueryErrorCode.java");
    private static final Path STATES = ROOT.resolve("odbc/SqlStateCode.java");

    static void compare(Map<Path, ClassTree> before, Map<Path, ClassTree> after, BiConsumer<Path, String> output) {
        Map<Integer, String> old = mapping(before);
        for (var entry : mapping(after).entrySet()) {
            String from = old.get(entry.getKey());
            if (from != null && !from.equals(entry.getValue()))
                output.accept(ERRORS, "Changed SQLState for error " + entry.getKey() + " from " + from + " to " + entry.getValue());
        }
    }

    private static Map<Integer, String> mapping(Map<Path, ClassTree> types) {
        ClassTree errors = types.get(ERRORS);
        ClassTree states = types.get(STATES);
        if (errors == null || states == null)
            return Map.of();
        Map<String, String> values = new HashMap<>();
        for (Tree member : states.getMembers()) {
            if (member instanceof VariableTree field && JavaContracts.literal(field.getInitializer()) instanceof String value)
                values.put(field.getName().toString(), value);
        }
        Map<String, Integer> codes = new HashMap<>();
        for (Tree member : errors.getMembers()) {
            if (member instanceof VariableTree field && field.getType().toString().equals("int")
                && field.getModifiers().getFlags().containsAll(Set.of(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL))
                && JavaContracts.literal(field.getInitializer()) instanceof Integer value)
                codes.put(field.getName().toString(), value);
        }
        for (Tree member : errors.getMembers()) {
            if (member instanceof MethodTree method && method.getName().contentEquals("codeToSqlState")
                && method.getParameters().size() == 1 && method.getParameters().get(0).getType().toString().equals("int")
                && method.getBody() != null && method.getBody().getStatements().size() == 1
                && method.getBody().getStatements().get(0) instanceof SwitchTree sw) {
                ExpressionTree selector = sw.getExpression();
                while (selector instanceof ParenthesizedTree parens)
                    selector = parens.getExpression();
                if (selector instanceof IdentifierTree id && id.getName().equals(method.getParameters().get(0).getName()))
                    return switchValues(sw, codes, values);
            }
        }
        return Map.of();
    }

    private static Map<Integer, String> switchValues(SwitchTree sw, Map<String, Integer> codes, Map<String, String> states) {
        Map<Integer, String> result = new TreeMap<>();
        List<Integer> pending = new ArrayList<>();
        boolean defaultFallsThrough = false;
        String fallback = null;
        for (CaseTree branch : sw.getCases()) {
            if (branch.getCaseKind() != CaseTree.CaseKind.STATEMENT)
                return Map.of();
            if (branch.getExpressions().isEmpty())
                defaultFallsThrough = true;
            for (ExpressionTree key : branch.getExpressions()) {
                if (!(key instanceof IdentifierTree id) || !codes.containsKey(id.getName().toString()))
                    return Map.of();
                pending.add(codes.get(id.getName().toString()));
            }
            var statements = branch.getStatements();
            if (statements.isEmpty())
                continue;
            if (statements.size() != 1 || !(statements.get(0) instanceof ReturnTree ret))
                return Map.of();
            ExpressionTree expr = ret.getExpression();
            String state = null;
            if (JavaContracts.literal(expr) instanceof String literal)
                state = literal;
            else if (expr instanceof MemberSelectTree ref && ref.getExpression().toString().equals("SqlStateCode"))
                state = states.get(ref.getIdentifier().toString());
            if (state == null)
                return Map.of();
            for (Integer code : pending)
                result.put(code, state);
            if (defaultFallsThrough) {
                fallback = state;
                defaultFallsThrough = false;
            }
            pending.clear();
        }
        if (fallback != null) {
            for (Integer code : codes.values())
                result.putIfAbsent(code, fallback);
        }
        return result;
    }
}
