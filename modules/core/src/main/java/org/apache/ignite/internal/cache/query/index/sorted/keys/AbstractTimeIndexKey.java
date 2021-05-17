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

import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.TimeInlineIndexKeyType;

/**
 * Abstract class for representing Time index key.
 *
 * {@link TimeInlineIndexKeyType} relies on this API to store an object in inline.
 */
public abstract class AbstractTimeIndexKey implements IndexKey {
    /** @return nanoseconds since midnight. */
    public abstract long nanos();

    /** {@inheritDoc} */
    @Override public int type() {
        return IndexKeyTypes.TIME;
    }
}
