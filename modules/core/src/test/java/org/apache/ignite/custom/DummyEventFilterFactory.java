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

package org.apache.ignite.custom;

import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;

/**
 * Must be not in org.apache.ignite.internal
 */
public class DummyEventFilterFactory<T> implements Factory<CacheEntryEventFilter<Integer, T>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public CacheEntryEventFilter<Integer, T> create() {
        return new DummyEventFilter<T>();
    }

    /**
     *
     */
    private static class DummyEventFilter<T> implements CacheEntryEventFilter<Integer, T> {
        /** {@inheritDoc} */
        @Override public boolean evaluate(
            final CacheEntryEvent<? extends Integer, ? extends T> evt) throws CacheEntryListenerException {
            return true;
        }
    }
}
