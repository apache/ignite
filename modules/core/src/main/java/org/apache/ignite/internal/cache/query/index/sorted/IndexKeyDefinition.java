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

package org.apache.ignite.internal.cache.query.index.sorted;

import org.apache.ignite.internal.cache.query.index.Order;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NullIndexKey;

/**
 * Defines a signle index key.
 */
public class IndexKeyDefinition {
    /** Index key name. */
    private final String name;

    /** Index key type. {@link IndexKeyTypes}. */
    private final int idxType;

    /** Order. */
    private final Order order;

    /** Precision for variable length key types. */
    private final int precision;

    /** */
    public IndexKeyDefinition(String name, int idxType, Order order, long precision) {
        this.idxType = idxType;
        this.order = order;
        this.name = name;

        // Workaround due to wrong type conversion (int -> long).
        if (precision >= Integer.MAX_VALUE)
            this.precision = -1;
        else
            this.precision = (int)precision;
    }

    /** */
    public Order order() {
        return order;
    }

    /** */
    public int idxType() {
        return idxType;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public int precision() {
        return precision;
    }

    /**
     * @return {@code true} if specified key's type matches to the current type, otherwise {@code false}.
     */
    public boolean validate(IndexKey key) {
        if (key == NullIndexKey.INSTANCE)
            return true;

        return idxType == key.type();
    }
}
