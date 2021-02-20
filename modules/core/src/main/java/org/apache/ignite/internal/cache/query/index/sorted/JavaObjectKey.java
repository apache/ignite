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

/**
 * Represents an index key that stores as Java Object.
 * Note, key can be as POJO or it can be well known java type (e.g. Integer) but stored explicitly as java object.
 *
 * {@link IndexKeyTypes#JAVA_OBJECT}.
 */
public class JavaObjectKey {
    /** Actual key that is used for indexing. */
    private final Object key;

    /** */
    public JavaObjectKey(Object o) {
        key = o;
    }

    /** Get wrapped key. */
    public Object getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return key.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return key.equals(obj);
    }
}
