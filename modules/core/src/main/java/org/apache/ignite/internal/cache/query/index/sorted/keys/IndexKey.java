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

package org.apache.ignite.internal.cache.query.index.sorted.keys;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;

/**
 * Class that represnts a single index key.
 */
public interface IndexKey {
    /**
     * @return Value of this key.
     */
    public Object key();

    /**
     * @return Index key type {@link IndexKeyType}.
     */
    public IndexKeyType type();

    /**
     * @return Comparison result with other IndexKey.
     */
    public int compare(IndexKey o) throws IgniteCheckedException;

    /**
     * @return {@code True} if index key can be compared to another index key (in case keys have different types).
     */
    public default boolean isComparableTo(IndexKey k) {
        return false;
    }
}
