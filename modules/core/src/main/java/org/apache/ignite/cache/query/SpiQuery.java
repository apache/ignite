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

package org.apache.ignite.cache.query;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.indexing.*;

/**
 * Query to be used by {@link IndexingSpi} implementations.
 *
 * @see IgniteCache#query(Query)
 * @see IgniteCache#localQuery(Query)
 */
public final class SpiQuery extends Query<SpiQuery> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Arguments. */
    @GridToStringInclude
    private Object[] args;

    /**
     * Gets SQL arguments.
     *
     * @return SQL arguments.
     */
    public Object[] getArgs() {
        return args;
    }

    /**
     * Sets SQL arguments.
     *
     * @param args SQL arguments.
     * @return {@code this} For chaining.
     */
    public SpiQuery setArgs(Object... args) {
        this.args = args;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SpiQuery.class, this);
    }
}
