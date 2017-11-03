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

/**
 * Arguments for fast, query-less UPDATE or DELETE - key and, optionally, value and new value.
 */
public final class FastUpdateArguments {
    /** Operand to compute key. */
    public final FastUpdateArgument key;

    /** Operand to compute value. */
    public final FastUpdateArgument val;

    /** Operand to compute new value. */
    public final FastUpdateArgument newVal;

    /** */
    public FastUpdateArguments(FastUpdateArgument key, FastUpdateArgument val, FastUpdateArgument newVal) {
        assert key != null && key != NULL_ARGUMENT;
        assert val != null;
        assert newVal != null;

        this.key = key;
        this.val = val;
        this.newVal = newVal;
    }

    /** Operand that always evaluates as {@code null}. */
    public final static FastUpdateArgument NULL_ARGUMENT = new FastUpdateArgument() {
        /** {@inheritDoc} */
        @Override public Object apply(Object[] arg) throws IgniteCheckedException {
            return null;
        }
    };
}
