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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.function.Function;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RuleSet;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.jetbrains.annotations.NotNull;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;

/**
 * Planning context.
 */
public final class PlanningContext implements Context {
    /** */
    private static final PlanningContext EMPTY = builder().build();

    /** */
    private final Context parentCtx;

    /** */
    private final String qry;

    /** */
    private final Object[] parameters;

    /** */
    private Function<RuleSet, RuleSet> rulesFilter;

    /** */
    private IgnitePlanner planner;

    /** */
    private CalciteConnectionConfig connCfg;

    /**
     * Private constructor, used by a builder.
     */
    private PlanningContext(
        Context parentCtx,
        String qry,
        Object[] parameters) {
        this.qry = qry;
        this.parameters = parameters;

        this.parentCtx = parentCtx;
    }

    /**
     * @return Query.
     */
    public String query() {
        return qry;
    }

    /**
     * @return Query parameters.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Object[] parameters() {
        return parameters;
    }

    // Helper methods
    /**
     * @return Sql operators table.
     */
    public SqlOperatorTable opTable() {
        return unwrap(QueryContextBase.class).config().getOperatorTable();
    }

    /**
     * @return Sql conformance.
     */
    public SqlConformance conformance() {
        return unwrap(QueryContextBase.class).config().getParserConfig().conformance();
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schema().getName();
    }

    /**
     * @return Schema.
     */
    public SchemaPlus schema() {
        return unwrap(QueryContextBase.class).config().getDefaultSchema();
    }

    /**
     * @return Planner.
     */
    public IgnitePlanner planner() {
        if (planner == null)
            planner = new IgnitePlanner(this);

        return planner;
    }

    /**
     * @return Type factory.
     */
    public IgniteTypeFactory typeFactory() {
        return unwrap(QueryContextBase.class).typeFactory();
    }

    /**
     * @return Cluster based on a planner and its configuration.
     */
    public RelOptCluster cluster() {
        return planner().cluster();
    }

    /** {@inheritDoc} */
    @Override public <C> C unwrap(Class<C> aCls) {
        if (aCls == getClass())
            return aCls.cast(this);

        if (aCls.isInstance(connCfg))
            return aCls.cast(connCfg);

        return parentCtx.unwrap(aCls);
    }

    /**
     * @return Context builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @return Empty context.
     */
    public static PlanningContext empty() {
        return EMPTY;
    }

    /** */
    public RuleSet rules(RuleSet set) {
        return rulesFilter != null ? rulesFilter.apply(set) : set;
    }

    /**
     * @param rulesFilter Rules filter.
     */
    public void rulesFilter(Function<RuleSet, RuleSet> rulesFilter) {
        this.rulesFilter = rulesFilter;
    }

    /**
     * Planner context builder.
     */
    @SuppressWarnings("PublicInnerClass") 
    public static class Builder {
        /** */
        private Context parentCtx = Contexts.empty();

        /** */
        private String qry;

        /** */
        private Object[] parameters;

        /**
         * @param parentCtx Parent context.
         * @return Builder for chaining.
         */
        public Builder parentContext(@NotNull Context parentCtx) {
            this.parentCtx = parentCtx;
            return this;
        }

        /**
         * @param qry Query.
         * @return Builder for chaining.
         */
        public Builder query(@NotNull String qry) {
            this.qry = qry;
            return this;
        }

        /**
         * @param parameters Query parameters.
         * @return Builder for chaining.
         */
        @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
        public Builder parameters(@NotNull Object... parameters) {
            this.parameters = parameters;
            return this;
        }

        /**
         * Builds planner context.
         *
         * @return Planner context.
         */
        public PlanningContext build() {
            return new PlanningContext(parentCtx, qry,
                parameters);
        }
    }
}
