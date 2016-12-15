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

package org.apache.ignite.internal.processors.hadoop.impl.v2;

import org.apache.ignite.hadoop.io.PartiallyRawComparator;
import org.apache.ignite.internal.processors.hadoop.io.OffheapRawMemory;
import org.apache.ignite.internal.processors.hadoop.io.PartiallyOffheapRawComparatorEx;

/**
 * Delegating partial raw comparator.
 */
public class HadoopV2DelegatingPartiallyOffheapRawComparator<T> implements PartiallyOffheapRawComparatorEx<T> {
    /** Target comparator. */
    private final PartiallyRawComparator<T> target;

    /** Memory. */
    private OffheapRawMemory mem;

    /**
     * Constructor.
     *
     * @param target Target.
     */
    public HadoopV2DelegatingPartiallyOffheapRawComparator(PartiallyRawComparator<T> target) {
        assert target != null;

        this.target = target;
    }

    /** {@inheritDoc} */
    @Override public int compare(T val1, long val2Ptr, int val2Len) {
        if (mem == null)
            mem = new OffheapRawMemory(val2Ptr, val2Len);
        else
            mem.update(val2Ptr, val2Len);

        return target.compare(val1, mem);
    }
}
