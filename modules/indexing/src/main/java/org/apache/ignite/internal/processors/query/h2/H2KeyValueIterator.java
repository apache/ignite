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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteBiTuple;

import java.sql.ResultSet;

/**
 * Special key/value iterator based on database result set.
 */
public class H2KeyValueIterator<K, V> extends H2ResultSetIterator<IgniteBiTuple<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param data Data array.
     * @throws IgniteCheckedException If failed.
     */
    protected H2KeyValueIterator(ResultSet data) throws IgniteCheckedException {
        super(data, false, true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteBiTuple<K, V> createRow() {
        K key = (K)row[0];
        V val = (V)row[1];

        return new IgniteBiTuple<>(key, val);
    }
}
