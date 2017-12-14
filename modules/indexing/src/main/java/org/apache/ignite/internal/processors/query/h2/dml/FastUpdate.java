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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.query.h2.UpdateResult;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlParameter;
import org.jetbrains.annotations.Nullable;

/**
 * Arguments for fast, query-less UPDATE or DELETE - key and, optionally, value and new value.
 */
public final class FastUpdate {
    /** Operand that always evaluates as {@code null}. */
    private final static FastUpdateArgument NULL_ARG = new ConstantArgument(null);

    /** Operand to compute key. */
    private final FastUpdateArgument keyArg;

    /** Operand to compute value. */
    private final FastUpdateArgument valArg;

    /** Operand to compute new value. */
    private final FastUpdateArgument newValArg;

    /**
     * Constructor.
     *
     * @param keyArg Key argument.
     * @param valArg Value argument.
     * @param newValArg New value argument.
     */
    private FastUpdate(FastUpdateArgument keyArg, FastUpdateArgument valArg, FastUpdateArgument newValArg) {
        this.keyArg = keyArg;
        this.valArg = valArg;
        this.newValArg = newValArg;
    }

    /**
     * Perform single cache operation based on given args.
     *
     * @param cache Cache.
     * @param args Query parameters.
     * @return 1 if an item was affected, 0 otherwise.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    public UpdateResult execute(GridCacheAdapter cache, Object[] args) throws IgniteCheckedException {
        Object key = keyArg.apply(args);

        assert key != null;

        Object val = valArg.apply(args);
        Object newVal = newValArg.apply(args);

        boolean res;

        if (newVal != null) {
            // Update.
            if (val != null)
                res = cache.replace(key, val, newVal);
            else
                res = cache.replace(key, newVal);
        }
        else {
            // Delete.
            if (val != null)
                res = cache.remove(key, val);
            else
                res = cache.remove(key);
        }

        return res ? UpdateResult.ONE : UpdateResult.ZERO;
    }

    /**
     * Create fast update instance.
     *
     * @param key Key element.
     * @param val Value element.
     * @param newVal New value element (if any)
     * @return Fast update.
     */
    public static FastUpdate create(GridSqlElement key, GridSqlElement val, @Nullable GridSqlElement newVal) {
        FastUpdateArgument keyArg = argument(key);
        FastUpdateArgument valArg = argument(val);
        FastUpdateArgument newValArg = argument(newVal);

        return new FastUpdate(keyArg, valArg, newValArg);
    }

    /**
     * Create argument for AST element.
     *
     * @param el Element.
     * @return Argument.
     */
    private static FastUpdateArgument argument(@Nullable GridSqlElement el) {
        assert el == null ^ (el instanceof GridSqlConst || el instanceof GridSqlParameter);

        if (el == null)
            return NULL_ARG;

        if (el instanceof GridSqlConst)
            return new ConstantArgument(((GridSqlConst)el).value().getObject());
        else
            return new ParamArgument(((GridSqlParameter)el).index());
    }

    /**
     * Value argument.
     */
    private static class ConstantArgument implements FastUpdateArgument {
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
        @Override public Object apply(Object[] arg) throws IgniteCheckedException {
            return val;
        }
    }

    /**
     * Parameter argument.
     */
    private static class ParamArgument implements FastUpdateArgument {
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
        @Override public Object apply(Object[] arg) throws IgniteCheckedException {
            assert arg.length > paramIdx;

            return arg[paramIdx];
        }
    }
}
