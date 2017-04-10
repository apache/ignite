/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.hibernate;

/**
 * Hibernate cache key wrapper.
 */
public class HibernateKeyWrapper {
    /** Key. */
    private final Object key;

    /** Entry. */
    private final String entry;

    /**
     * @param key Key.
     * @param entry Entry.
     */
    public HibernateKeyWrapper(Object key, String entry) {
        this.key = key;
        this.entry = entry;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        HibernateKeyWrapper wrapper = (HibernateKeyWrapper)o;

        if (key != null ? !key.equals(wrapper.key) : wrapper.key != null)
            return false;

        return entry != null ? entry.equals(wrapper.entry) : wrapper.entry == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = key != null ? key.hashCode() : 0;

        result = 31 * result + (entry != null ? entry.hashCode() : 0);

        return result;
    }
}
