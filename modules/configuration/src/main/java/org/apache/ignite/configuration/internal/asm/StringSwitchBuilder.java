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

package org.apache.ignite.configuration.internal.asm;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.control.SwitchStatement.SwitchBuilder;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.facebook.presto.bytecode.BytecodeUtils.checkState;
import static com.facebook.presto.bytecode.control.SwitchStatement.switchBuilder;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.IntStream.range;

/**
 * Class similar to {@link SwitchBuilder} but it allows to use {@link String}s as switch keys.
 */
class StringSwitchBuilder {
    /** {@link Object#equals(Object)} */
    private static final Method EQUALS;

    /** {@link Object#hashCode()} */
    private static final Method HASH_CODE;

    static {
        try {
            EQUALS = Object.class.getDeclaredMethod("equals", Object.class);

            HASH_CODE = Object.class.getDeclaredMethod("hashCode");
        }
        catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Expression to be used for matching. */
    private BytecodeExpression expression;

    /** Set of case statements in order of appearance. */
    private final Set<CaseStatement> cases = new LinkedHashSet<>();

    /** Default section for the switch. */
    private BytecodeNode defaultBody;

    /** Scope to create temp variables. */
    private final Scope scope;

    /**
     * @param scope Scope to create temp variables.
     */
    StringSwitchBuilder(Scope scope) {
        this.scope = scope;
    }

    /**
     * @param expression Expression to be used for matching.
     * @return {@code this} for chaining.
     */
    public StringSwitchBuilder expression(BytecodeExpression expression) {
        this.expression = expression;
        return this;
    }

    /**
     * @param key Key for matching.
     * @param body Case statement.
     * @return {@code this} for chaining.
     */
    public StringSwitchBuilder addCase(String key, BytecodeNode body) {
        CaseStatement statement = new CaseStatement(key, body);

        checkState(cases.add(statement), "case already exists for value [%s]", key);

        return this;
    }

    /**
     * @param body Default statement.
     * @return {@code this} for chaining.
     */
    public StringSwitchBuilder defaultCase(BytecodeNode body) {
        checkState(defaultBody == null, "default case already set");
        this.defaultBody = requireNonNull(body, "body is null");
        return this;
    }

    /**
     * Produces "compiled" switch statement, ready to be inserted into method.
     * @return Bytecode block.
     */
    public BytecodeBlock build() {
        checkState(expression != null, "expression is not set");

        // Variable to cache the expression string.
        Variable expVar = scope.createTempVariable(String.class);

        // Variable to store integer index corresponding to the matched string.
        Variable idxVar = scope.createTempVariable(int.class);

        BytecodeBlock res = new BytecodeBlock()
            .append(expVar.set(expression)) // expVar = evaluate(expression);
            .append(idxVar.set(constantInt(-1))); // idxVar = -1;

        BytecodeNode[] caseBodies = new BytecodeNode[cases.size()];

        // Here we are preparing case statements for the first switch. It'll look like this:
        // switch (expVar.hashCode()) {
        //     case <hash1>:
        //         if (expVar.equals("a")) idxVar = 0;
        //     case <hash2>:
        //         if (expVar.equals("b")) idxVar = 1;
        //         if (expVar.equals("c")) idxVar = 2;
        //     ...
        // }
        SwitchBuilder hashSwitch = switchBuilder()
            .expression(expVar.invoke(HASH_CODE));

        // Case for each hash value may have multiple matching strings due to collisions.
        Map<Integer, List<CaseStatement>> groupedCases = cases.stream().collect(
            groupingBy(statement -> statement.key.hashCode())
        );

        int idx = 0;
        for (Map.Entry<Integer, List<CaseStatement>> entry : groupedCases.entrySet()) {
            BytecodeBlock caseBody = new BytecodeBlock();

            for (CaseStatement caseStmt : entry.getValue()) {
                caseBody.append(new IfStatement()
                    .condition(expVar.invoke(EQUALS, constantString(caseStmt.key)))
                    .ifTrue(idxVar.set(constantInt(idx)))
                );

                caseBodies[idx++] = caseStmt.body;
            }

            hashSwitch.addCase(entry.getKey(), caseBody);
        }

        // No default statement required because "idxVar" was preemptively assigned to -1.
        res.append(hashSwitch.build());

        // Here's the actual switch by "idxVar" variable that uses user-defined "case" and "default" clauses.
        res.append(range(0, caseBodies.length).boxed()
            .reduce(
                switchBuilder().expression(idxVar),
                (builder, i) -> builder.addCase(i, caseBodies[i]),
                (b0, b1) -> b0 // Won't be used by sequential stream but required to be non-null.
            )
            .defaultCase(defaultBody)
            .build()
        );

        return res;
    }

    /**
     * Case statement class for the builder.
     */
    private static class CaseStatement {
        /** String key of the case statement. */
        private final String key;

        /** Body of the case statement. */
        private final BytecodeNode body;

        /**
         * @param key String key of the case statement.
         * @param body Body of the case statement.
         */
        CaseStatement(String key, BytecodeNode body) {
            this.key = key;
            this.body = requireNonNull(body, "body is null");
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (this == obj)
                return true;

            if (obj == null || getClass() != obj.getClass())
                return false;

            CaseStatement other = (CaseStatement)obj;
            return Objects.equals(this.key, other.key);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return getClass().getSimpleName() + "[key=" + key + ']';
        }
    }
}
