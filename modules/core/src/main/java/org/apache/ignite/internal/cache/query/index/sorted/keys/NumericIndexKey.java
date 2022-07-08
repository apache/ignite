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

/**
 * Class that represnts a numeric index key.
 */
public abstract class NumericIndexKey implements IndexKey {
    /** */
    public abstract int compareTo(boolean val);

    /** */
    public abstract int compareTo(byte val);

    /** */
    public abstract int compareTo(short val);

    /** */
    public abstract int compareTo(int val);

    /** */
    public abstract int compareTo(long val);

    /** */
    public abstract int compareTo(float val);

    /** */
    public abstract int compareTo(double val);

    /** */
    public abstract int compareTo(BigDecimal val);

    /** {@inheritDoc} */
    @Override public boolean isComparableTo(IndexKey k) {
        return k instanceof NumericIndexKey;
    }
}
