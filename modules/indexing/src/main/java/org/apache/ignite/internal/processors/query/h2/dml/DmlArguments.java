/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.dml;

import org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlParameter;
import org.jetbrains.annotations.Nullable;

/**
 * DML arguments factory.
 */
public class DmlArguments {
    /** Operand that always evaluates as {@code null}. */
    private static final DmlArgument NULL_ARG = new ConstantArgument(null);

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

        if (el instanceof GridSqlConst)
            return new ConstantArgument(((GridSqlConst)el).value().getObject());
        else
            return new ParamArgument(((GridSqlParameter)el).index());
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

        /**
         * Constructor.
         *
         * @param val Value.
         */
        private ConstantArgument(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        public Object get(Object[] params) {
            return val;
        }
    }

    /**
     * Parameter argument.
     */
    private static class ParamArgument implements DmlArgument {
        /** Value to return. */
        private final int paramIdx;

        /**
         * Constructor.
         *
         * @param paramIdx Parameter index.
         */
        private ParamArgument(int paramIdx) {
            assert paramIdx >= 0;

            this.paramIdx = paramIdx;
        }

        /** {@inheritDoc} */
        @Override public Object get(Object[] params) {
            assert params.length > paramIdx;

            return params[paramIdx];
        }
    }
}
