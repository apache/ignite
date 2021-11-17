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
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RuleSet;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.BaseQueryContext;
import org.jetbrains.annotations.NotNull;

/**
 * Planning context.
 */
public final class PlanningContext implements Context {
    private final Context parentCtx;

    private final String qry;

    private final Object[] parameters;

    private Function<RuleSet, RuleSet> rulesFilter;

    private IgnitePlanner planner;

    /**
     * Private constructor, used by a builder.
     */
    private PlanningContext(
            Context parentCtx,
            String qry,
            Object[] parameters
    ) {
        this.qry = qry;
        this.parameters = parameters;
        this.parentCtx = parentCtx;
    }

    /**
     * Get framework config.
     */
    public FrameworkConfig config() {
        return unwrap(BaseQueryContext.class).config();
    }

    /**
     * Get query.
     */
    public String query() {
        return qry;
    }

    /**
     * Get query parameters.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Object[] parameters() {
        return parameters;
    }

    // Helper methods

    /**
     * Get sql operators table.
     */
    public SqlOperatorTable opTable() {
        return config().getOperatorTable();
    }

    /**
     * Get sql conformance.
     */
    public SqlConformance conformance() {
        return config().getParserConfig().conformance();
    }

    /**
     * Get planner.
     */
    public IgnitePlanner planner() {
        if (planner == null) {
            planner = new IgnitePlanner(this);
        }

        return planner;
    }

    /**
     * Get schema name.
     */
    public String schemaName() {
        return schema().getName();
    }

    /**
     * Get schema.
     */
    public SchemaPlus schema() {
        return config().getDefaultSchema();
    }

    /**
     * Get type factory.
     */
    public IgniteTypeFactory typeFactory() {
        return unwrap(BaseQueryContext.class).typeFactory();
    }

    /**
     * Get new catalog reader.
     */
    public CalciteCatalogReader catalogReader() {
        return unwrap(BaseQueryContext.class).catalogReader();
    }

    /**
     * Get cluster based on a planner and its configuration.
     */
    public RelOptCluster cluster() {
        return planner().cluster();
    }

    /** {@inheritDoc} */
    @Override
    public <C> C unwrap(Class<C> clazz) {
        if (clazz == getClass()) {
            return clazz.cast(this);
        }

        return parentCtx.unwrap(clazz);
    }

    /**
     * Get context builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    public RuleSet rules(RuleSet set) {
        return rulesFilter != null ? rulesFilter.apply(set) : set;
    }

    /**
     * Set rules filter.
     */
    public void rulesFilter(Function<RuleSet, RuleSet> rulesFilter) {
        this.rulesFilter = rulesFilter;
    }

    /**
     * Planner context builder.
     */
    public static class Builder {
        private Context parentCtx = Contexts.empty();

        private String qry;

        private Object[] parameters;

        public Builder parentContext(@NotNull Context parentCtx) {
            this.parentCtx = parentCtx;
            return this;
        }

        public Builder query(@NotNull String qry) {
            this.qry = qry;
            return this;
        }

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
            return new PlanningContext(parentCtx, qry, parameters);
        }
    }
}
