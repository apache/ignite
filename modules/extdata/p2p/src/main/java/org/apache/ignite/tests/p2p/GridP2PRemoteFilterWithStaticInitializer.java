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

package org.apache.ignite.tests.p2p;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;

/**
 * Event filter with static initalizer.
 */
public class GridP2PRemoteFilterWithStaticInitializer implements CacheEntryEventFilter<Integer, Integer> {
    /** */
    private final GridP2PTestObjectWithStaticInitializer testObj = new GridP2PTestObjectWithStaticInitializer();

    /** {@inheritDoc} */
    @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends Integer> evt)
        throws CacheEntryListenerException {
        return evt.getValue() == null || evt.getValue() % 2 != 0;
    }
}
