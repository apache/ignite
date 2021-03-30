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

package org.apache.ignite.internal.metastorage.server;

import org.jetbrains.annotations.NotNull;

/**
 * Condition tests an entry on existence in meta storage.
 * Entry exists if it is not empty and not tombstone.
 */
public class ExistenceCondition extends AbstractCondition {
    /** Condition type. */
    @NotNull
    private final Type type;

    /**
     * Constructs existence condition with the given type and for entry identified by the given key.
     *
     * @param type Condition type. Can't be {@code null}.
     * @param key Key of entry which condition will be applied to. Can't be {@code null}.
     */
    public ExistenceCondition(@NotNull Type type, @NotNull byte[] key) {
        super(key);

        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public boolean test(@NotNull Entry e) {
        boolean res = !(e.empty() || e.tombstone());

        return type.test(res);
    }

    /** Defines existence condition types. */
    public enum Type {
        /** Equality condition type. */
        EXISTS {
            @Override public boolean test(boolean res) {
                return res;
            }
        },

        /** Inequality condition type. */
        NOT_EXISTS {
            @Override public boolean test(boolean res) {
                return !res;
            }
        };

        /**
         * Interprets comparison result.
         *
         * @param res The result of comparison.
         * @return The interpretation of the comparison result.
         */
        public abstract boolean test(boolean res);
    }
}
