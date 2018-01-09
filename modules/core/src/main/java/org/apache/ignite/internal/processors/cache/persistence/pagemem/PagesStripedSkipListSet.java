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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;

/**
 *
 */
public class PagesStripedSkipListSet extends PagesConcurrentHashSet {
    /**
     * @return created new set for bucket data storage.
     */
    @Override protected Set<FullPageId> createNewSet() {
        return new ConcurrentSkipListSet<>(GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR);
    }
}
