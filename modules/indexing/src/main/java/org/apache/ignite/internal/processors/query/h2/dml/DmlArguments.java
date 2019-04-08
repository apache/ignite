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

package org.apache.ignite.internal.processors.query.h2.dml;

import org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlParameter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlType;
import org.jetbrains.annotations.Nullable;

/**
 * DML arguments factory.
 */
public class DmlArguments {
    /** Operand that always evaluates as {@code null}. */
    private static final DmlArgument NULL_ARG = new ConstantArgument(null, GridSqlType.NULL);

    /**
     * Create argument from AST element.
     *
     * @param el Element.
     * @return DML argument.
     */
    public static DmlArgument create(@Nullable GridSqlElement el) {
        assert el == null ^ (el instanceof GridSqlConst || el instanceof GridSqlParameter);

        if (el == null)
            return NULL_ARG;

        if (el instanceof GridSqlConst) {
            GridSqlConst constVal = ((GridSqlConst)el);

            return new ConstantArgument(constVal.value().getObject(), constVal.resultType());
        }
        else {
            GridSqlParameter param = (GridSqlParameter)el;

            return new ParamArgument(param.index(), param.resultType());
        }
    }

    /**
     * Private constructor.
     */
    private DmlArguments() {
        // No-op.
    }

    /**
     * Value argument.
     */
    private static class ConstantArgument implements DmlArgument {
        /** Value to return. */
        private final Object val;

        /** H2 type of this constant. */
        private final GridSqlType type;

        /**
         * Constructor.
         *  @param val Value.
         * @param type H2 type of this constant.
         */
        private ConstantArgument(Object val, GridSqlType type) {
            this.val = val;
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public Object get(Object[] params) {
            return val;
        }

        /** {@inheritDoc} */
        @Override public GridSqlType expectedType() {
            return type;
        }
    }

    /**
     * Parameter argument.
     */
    private static class ParamArgument implements DmlArgument {
        /** Value to return. */
        private final int paramIdx;

        /** H2 type of this parameter. */
        private final GridSqlType type;

        /**
         * Constructor.
         *
         * @param paramIdx Parameter index.
         * @param type H2 type of this parameter.
         */
        private ParamArgument(int paramIdx, GridSqlType type) {
            assert paramIdx >= 0;

            this.paramIdx = paramIdx;
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public Object get(Object[] params) {
            assert params.length > paramIdx;

            return params[paramIdx];
        }

        @Override public GridSqlType expectedType() {
            return type;
        }
    }
}
