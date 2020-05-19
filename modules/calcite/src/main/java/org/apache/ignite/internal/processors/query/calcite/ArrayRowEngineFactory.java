/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite;

import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ArrayExpressionFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/**
 * Array row engine factory.
 */
public class ArrayRowEngineFactory implements RowEngineFactory<Object[]> {
    /** */
    private static final RowEngine<Object[]> ARRAY_ROW_ENGINE = new ArrayRowEngine();

    /** {@inheritDoc} */
    @Override public RowEngine<Object[]>  rowEngine() {
        return ARRAY_ROW_ENGINE;
    }

    /** */
    private static class ArrayRowEngine implements RowEngine<Object[]> {
        /** {@inheritDoc} */
        @Override public RowHandler<Object[]> rowHandler() {
            return ArrayRowHandler.INSTANCE;
        }

        /** {@inheritDoc} */
        @Override public ExpressionFactory<Object[]> expressionFactory(IgniteTypeFactory typeFactory,
            SqlConformance conformance) {
            return new ArrayExpressionFactory(typeFactory, conformance);
        }
    }
}
