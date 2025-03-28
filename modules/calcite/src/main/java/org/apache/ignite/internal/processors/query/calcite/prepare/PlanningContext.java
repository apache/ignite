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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.util.CancelFlag;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Planning context.
 */
public final class PlanningContext implements Context {
    /** */
    private final Context parentCtx;

    /** */
    private final String qry;

    /** */
    @Nullable private final Object[] parameters;

    /** */
    private final CancelFlag cancelFlag = new CancelFlag(new AtomicBoolean());

    /** */
    private Function<RuleSet, RuleSet> rulesFilter;

    /** */
    private IgnitePlanner planner;

    /** */
    private final long startTs;

    /** */
    private final long plannerTimeout;

    /**
     * Private constructor, used by a builder.
     */
    private PlanningContext(
        Context parentCtx,
        String qry,
        @Nullable Object[] parameters,
        long plannerTimeout
    ) {
        this.qry = qry;
        this.parameters = parameters;

        this.parentCtx = parentCtx;
        startTs = U.currentTimeMillis();
        this.plannerTimeout = plannerTimeout;
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
    @Nullable public Object[] parameters() {
        return parameters;
    }

    // Helper methods
    /**
     * @return Sql conformance.
     */
    public SqlConformance conformance() {
        return config().getParserConfig().conformance();
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schema().getName();
    }

    /**
     * @return Start timestamp in millis.
     */
    public long startTs() {
        return startTs;
    }

    /**
     * @return Planning timeout in millis.
     */
    public long plannerTimeout() {
        return plannerTimeout;
    }

    /**
     * @return Schema.
     */
    public SchemaPlus schema() {
        return config().getDefaultSchema();
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
        return unwrap(BaseQueryContext.class).typeFactory();
    }

    /**
     * @return Sql operators table.
     */
    public SqlOperatorTable opTable() {
        return unwrap(BaseQueryContext.class).opTable();
    }

    /**
     * @return New catalog reader.
     */
    public CalciteCatalogReader catalogReader() {
        return unwrap(BaseQueryContext.class).catalogReader();
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

        if (aCls == CancelFlag.class)
            return aCls.cast(cancelFlag);

        return parentCtx.unwrap(aCls);
    }

    /**
     * @return Context builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** */
    public RuleSet rules(RuleSet set) {
        return rulesFilter != null ? rulesFilter.apply(set) : set;
    }

    /**
     * Sets a rule filter. If already exists, enqueues the new one after the current filter.
     *
     * @param rulesFilter Rules filter.
     */
    public void addRulesFilter(Function<RuleSet, RuleSet> rulesFilter) {
        this.rulesFilter = this.rulesFilter == null ? rulesFilter : this.rulesFilter.andThen(rulesFilter);
    }

    /**
     * @return Framework config.
     */
    public FrameworkConfig config() {
        return unwrap(BaseQueryContext.class).config();
    }

    /** */
    public RexBuilder rexBuilder() {
        return unwrap(BaseQueryContext.class).rexBuilder();
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

        /** */
        private long plannerTimeout;

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
         * @param plannerTimeout Planner timeout.
         *
         * @return Builder for chaining.
         */
        public Builder plannerTimeout(long plannerTimeout) {
            this.plannerTimeout = plannerTimeout;
            return this;
        }

        /**
         * Builds planner context.
         *
         * @return Planner context.
         */
        public PlanningContext build() {
            return new PlanningContext(parentCtx, qry, parameters, plannerTimeout);
        }
    }
}
