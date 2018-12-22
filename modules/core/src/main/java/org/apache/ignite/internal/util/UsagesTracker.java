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

package org.apache.ignite.internal.util;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Usages tracker.
 * <p/>
 * Can not be less than zero even if 'decrement' method has been called more times than 'increment'.
 */
public class UsagesTracker implements UsagesTrackable, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final AtomicInteger usagesCounter = new AtomicInteger(0);

    /** {@inheritDoc} */
    @Override public int incrementAndGet() {
        return usagesCounter.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public int decrementAndGet() {
        return usagesCounter.updateAndGet(i -> i > 0 ? i - 1 : i);
    }

    /** {@inheritDoc} */
    @Override public int get() {
        return usagesCounter.get();
    }
}
