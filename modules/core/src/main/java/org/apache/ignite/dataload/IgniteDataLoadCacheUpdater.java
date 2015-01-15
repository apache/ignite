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

package org.apache.ignite.dataload;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;

import java.io.*;
import java.util.*;

/**
 * Updates cache with batch of entries. Usually it is enough to configure {@link org.apache.ignite.IgniteDataLoader#isolated(boolean)}
 * property and appropriate internal cache updater will be chosen automatically. But in some cases to achieve best
 * performance custom user-defined implementation may help.
 * <p>
 * Data loader can be configured to use custom implementation of updater instead of default one using
 * {@link org.apache.ignite.IgniteDataLoader#updater(IgniteDataLoadCacheUpdater)} method.
 */
public interface IgniteDataLoadCacheUpdater<K, V> extends Serializable {
    /**
     * Updates cache with batch of entries.
     *
     * @param cache Cache.
     * @param entries Collection of entries.
     * @throws IgniteCheckedException If failed.
     */
    public void update(GridCache<K, V> cache, Collection<Map.Entry<K, V>> entries) throws IgniteCheckedException;
}
