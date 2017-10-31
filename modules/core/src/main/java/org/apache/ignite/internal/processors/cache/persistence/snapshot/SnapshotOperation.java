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
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import java.util.Set;

/**
 * Initial snapshot operation interface.
 */
public interface SnapshotOperation extends Serializable {
    /**
     * Cache group ids included to this snapshot.
     *
     * @return Cache names.
     */
    public Set<Integer> cacheGroupIds();

    /**
     * Cache names included to this snapshot.
     */
    public Set<String> cacheNames();

    /**
     * Any custom extra parameter.
     */
    public Object extraParameter();
}
