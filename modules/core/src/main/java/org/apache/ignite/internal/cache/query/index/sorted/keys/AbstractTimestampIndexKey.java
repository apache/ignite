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
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.TimestampInlineIndexKeyType;

/**
 * Abstract class for representing Timestamp index key.
 *
 * {@link TimestampInlineIndexKeyType} relies on this API to store an object in inline.
 */
public abstract class AbstractTimestampIndexKey implements IndexKey {
    /** @return a date value {@link DateValueUtils}. */
    public abstract long getDateValue();

    /** @return nanoseconds since midnight. */
    public abstract long getNanos();

    /** {@inheritDoc} */
    @Override public int getType() {
        return IndexKeyTypes.TIMESTAMP;
    }
}
