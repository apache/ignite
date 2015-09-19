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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

/**
 * Query execution bean.
 */
public class GridCacheQueryBean {
    /** */
    private final GridCacheQueryAdapter<?> qry;

    /** */
    private final IgniteReducer<Object, Object> rdc;

    /** */
    private final IgniteClosure<Object, Object> trans;

    /** */
    private final Object[] args;

    /**
     * @param qry Query.
     * @param rdc Optional reducer.
     * @param trans Optional transformer.
     * @param args Optional arguments.
     */
    public GridCacheQueryBean(GridCacheQueryAdapter<?> qry, @Nullable IgniteReducer<Object, Object> rdc,
        @Nullable IgniteClosure<Object, Object> trans, @Nullable Object[] args) {
        assert qry != null;

        this.qry = qry;
        this.rdc = rdc;
        this.trans = trans;
        this.args = args;
    }

    /**
     * @return Query.
     */
    public GridCacheQueryAdapter<?> query() {
        return qry;
    }

    /**
     * @return Reducer.
     */
    @Nullable public IgniteReducer<Object, Object> reducer() {
        return rdc;
    }

    /**
     * @return Transformer.
     */
    @Nullable public IgniteClosure<Object, Object> transform() {


        return trans;
    }

    /**
     * @return Arguments.
     */
    @Nullable public Object[] arguments() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryBean.class, this);
    }
}