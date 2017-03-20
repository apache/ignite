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

package org.apache.ignite.internal.processors.query.ddl.task;

import org.apache.ignite.internal.processors.query.QueryIndexStates;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Indexing cache start task.
 */
public class IndexingCacheStartTask implements IndexingTask {
    /** Space. */
    private final String space;

    /** Initial index states. */
    private final QueryIndexStates initIdxStates;

    /**
     * Constructor.
     *
     * @param space Space.
     * @param initIdxStates Initial index states.
     */
    public IndexingCacheStartTask(String space, QueryIndexStates initIdxStates) {
        this.space = space;
        this.initIdxStates = initIdxStates;
    }

    /**
     * @return Space.
     */
    public String space() {
        return space;
    }

    /**
     * @return Initial index states.
     */
    public QueryIndexStates initialIndexStates() {
        return initIdxStates;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexingCacheStartTask.class, this);
    }
}
