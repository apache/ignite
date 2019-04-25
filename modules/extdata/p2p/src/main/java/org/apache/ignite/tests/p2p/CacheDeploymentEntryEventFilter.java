/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
 * Event filter for deployment.
 */
public class CacheDeploymentEntryEventFilter implements CacheEntryEventFilter<Integer, Integer> {
    /** {@inheritDoc} */
    @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends Integer> evt)
        throws CacheEntryListenerException {
        return evt.getValue() == null || evt.getValue() % 2 != 0;
    }
}
