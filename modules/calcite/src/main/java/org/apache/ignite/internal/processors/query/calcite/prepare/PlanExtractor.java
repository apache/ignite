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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsProcessor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.util.typedef.F;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Sensitive data aware plan extractor.
 */
public class PlanExtractor {
    /** */
    private final PerformanceStatisticsProcessor perfStatProc;

    /** */
    public PlanExtractor(GridKernalContext ctx) {
        perfStatProc = ctx.performanceStatistics();
    }

    /** */
    public String extract(IgniteRel rel) {
        if (QueryUtils.INCLUDE_SENSITIVE)
            return RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES);
        else {
            StringWriter sw = new StringWriter();
            RelWriter planWriter = new SensitiveDataAwarePlanWriter(new PrintWriter(sw));
            rel.explain(planWriter);
            return sw.toString();
        }
    }

    /** */
    private static final class LiteralRemoveShuttle extends RexShuttle {
        /** */
        private static final LiteralRemoveShuttle INSTANCE = new LiteralRemoveShuttle();

        /** {@inheritDoc} */
        @Override public RexNode visitLiteral(RexLiteral literal) {
            return new RexDynamicParam(literal.getType(), -1);
        }
    }

    /** */
    private static class SensitiveDataAwarePlanWriter extends RelWriterImpl {
        /** */
        public SensitiveDataAwarePlanWriter(PrintWriter pw) {
            super(pw, SqlExplainLevel.ALL_ATTRIBUTES, false);
        }

        /** {@inheritDoc} */
        @Override public RelWriter item(String term, @Nullable Object val) {
            return super.item(term, removeSensitive(val));
        }

        /** */
        private Object removeSensitive(Object val) {
            if (val instanceof RexNode)
                return LiteralRemoveShuttle.INSTANCE.apply((RexNode)val);
            else if (val instanceof Collection)
                return F.transform((Collection<?>)val, this::removeSensitive);
            else if (val instanceof SearchBounds)
                return ((SearchBounds)val).transform(LiteralRemoveShuttle.INSTANCE::apply);

            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean nest() {
            // Don't try to expand some values by rel nodes, use original values.
            return true;
        }
    }
}
