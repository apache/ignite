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

package org.apache.ignite.internal.util.lang.gridfunc;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteReducer;

/**
 * Reducer that calculates sum of long integer elements.
 */
public class LongSumReducer implements IgniteReducer<Long, Long> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private AtomicLong sum = new AtomicLong(0);

    /** {@inheritDoc} */
    @Override public boolean collect(Long e) {
        if (e != null)
            sum.addAndGet(e);

        return true;
    }

    /** {@inheritDoc} */
    @Override public Long reduce() {
        return sum.get();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LongSumReducer.class, this);
    }
}
