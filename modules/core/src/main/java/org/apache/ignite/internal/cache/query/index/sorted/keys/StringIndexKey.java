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

import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;

/** */
public class StringIndexKey implements IndexKey {
    /** */
    private final String key;

    /** */
    public StringIndexKey(String key) {
        this.key = key;
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public IndexKeyType type() {
        return IndexKeyType.STRING;
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o) {
        String okey = (String)o.key();

        return key.compareTo(okey);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return key;
    }

//    /** {@inheritDoc} */
//    @Override public int hashCode() {
//        return key.hashCode();
//    }
//
//    /** {@inheritDoc} */
//    @Override public boolean equals(Object o) {
//        return o instanceof StringIndexKey && compare((IndexKey)o) == 0;
//    }

}

