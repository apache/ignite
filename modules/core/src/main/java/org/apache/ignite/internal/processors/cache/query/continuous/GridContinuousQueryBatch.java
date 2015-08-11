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

package org.apache.ignite.internal.processors.cache.query.continuous;

import org.apache.ignite.internal.processors.continuous.*;
import org.jsr166.*;

import java.util.*;

/**
 * Continuous query batch.
 */
class GridContinuousQueryBatch extends GridContinuousBatchAdapter {
    /** Update indexes. */
    private final Map<Integer, Long> updateIdxs = new ConcurrentHashMap8<>();

    /**
     * @return Update indexes.
     */
    Map<Integer, Long> updateIndexes() {
        return updateIdxs;
    }

    /** {@inheritDoc} */
    @Override public void add(Object obj) {
        super.add(obj);

        CacheContinuousQueryEntry entry = (CacheContinuousQueryEntry)obj;

        updateIdxs.put(entry.partition(), entry.updateIndex());
    }
}
