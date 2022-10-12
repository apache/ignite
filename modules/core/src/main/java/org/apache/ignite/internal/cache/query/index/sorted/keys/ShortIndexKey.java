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

import java.math.BigDecimal;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;

/** */
public class ShortIndexKey extends NumericIndexKey {
    /** */
    private final short key;

    /** */
    public ShortIndexKey(short key) {
        this.key = key;
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public IndexKeyType type() {
        return IndexKeyType.SHORT;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(boolean val) {
        return Boolean.compare(key != (short)0, val);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(byte val) {
        return Integer.compare(key, val);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(short val) {
        return Integer.compare(key, val);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(int val) {
        return Integer.compare(key, val);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(long val) {
        return Long.compare(key, val);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(float val) {
        return Float.compare(key, val);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(double val) {
        return Double.compare(key, val);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(BigDecimal val) {
        return BigDecimal.valueOf(key).compareTo(val);
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o) {
        return -((NumericIndexKey)o).compareTo(key);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return String.valueOf(key);
    }
}
