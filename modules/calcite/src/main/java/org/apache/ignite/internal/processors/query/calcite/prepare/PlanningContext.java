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
import org.apache.ignite.internal.util.lang.IgnitePair;
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
    private final Object[] parameters;

    /**
     * If not {@code null}, notifies to validate passed parameters number against number of the query's dynamic parameters.
     * Since several queries may share {@link #parameters()} while each query is validated by dedicated validator,
     * this validator has to be aware of numbers of total queries and current query.
     * The pair is: number of current query starting with 0 and total number of queries.
     */
    @Nullable private final IgnitePair<Integer> validateParamsNumberCfg;

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
     *
     * @param parentCtx Parent context.
     * @param qry The query to process.
     * @param parameters Parameters to pass to the query dynamic parameters.
     * @param validateParamsNumberCfg If not {@code null}, notifies to validate passed parameters number against number
     *                                of the query's dynamic parameters. The pair is: number of current query starting with 0
     *                                and total number of queries to process.
     * @param plannerTimeout Timeout on operation.
     *
     * @see #validateParamsNumberCfg
     */
    private PlanningContext(
        Context parentCtx,
        String qry,
        Object[] parameters,
        @Nullable IgnitePair<Integer> validateParamsNumberCfg,
        long plannerTimeout
    ) {
        this.qry = qry;
        this.parameters = parameters;
        this.validateParamsNumberCfg = validateParamsNumberCfg;

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
    public Object[] parameters() {
        return parameters;
    }

    /** @return {@code True}, if the validation of number of {@link #parameters()} is required. */
    public boolean validateParamsNumber() {
        return validateParamsNumberCfg != null;
    }

    /**
     * @return Number of current query being processed with the same {@link #parameters()}. Starts with 0 and is always 0
     * if {@link #validateParamsNumber()} is not set.
     * @see #validateParamsNumber()
     */
    public int currentQueryNumber() {
        return validateParamsNumberCfg == null ? 0 : validateParamsNumberCfg.get1();
    }

    /**
     * @return Total number of queries to process with the same {@link #parameters()}. Is always 0 if {@link #validateParamsNumber()}
     * is not set.
     * @see #validateParamsNumber()
     */
    public int queriesCnt() {
        return validateParamsNumberCfg == null ? 0 : validateParamsNumberCfg.get2();
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
     * @param rulesFilter Rules filter.
     */
    public void rulesFilter(Function<RuleSet, RuleSet> rulesFilter) {
        this.rulesFilter = rulesFilter;
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
        private int curQryNum;

        /** */
        private int totalQueriesCnt;

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
         * @param curQryNum Number of curent query being processed with shared {@link #parameters(Object...)}.
         *                  Starts with 0.
         * @return Builder for chaining.
         * @see PlanningContext#validateParamsNumber()
         * @see #validateParametersTotalQueries(int)
         */
        public Builder validateParametersQueryNumber(int curQryNum) {
            this.curQryNum = curQryNum;
            return this;
        }

        /**
         * @param totalQueriesCnt Total number of quries being processed with the same {@link #parameters(Object...)}.
         * @return Builder for chaining.
         * @see PlanningContext#validateParamsNumber()
         * @see #validateParametersQueryNumber(int)
         */
        public Builder validateParametersTotalQueries(int totalQueriesCnt) {
            this.totalQueriesCnt = totalQueriesCnt;
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
            IgnitePair validateParamsNumCfg = totalQueriesCnt != 0 || curQryNum != 0 ? new IgnitePair(curQryNum, totalQueriesCnt) : null;

            return new PlanningContext(parentCtx, qry, parameters, validateParamsNumCfg, plannerTimeout);
        }
    }
}
