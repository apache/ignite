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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Compound snapshot verification exception from the nodes where the verification process executed.
 */
public class IgniteSnapshotVerifyException extends IgniteException {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Map of received exceptions. */
    private final Map<ClusterNode, Exception> exs = new HashMap<>();

    /**
     * @param map Map of received exceptions.
     */
    public IgniteSnapshotVerifyException(Map<ClusterNode, ? extends Exception> map) {
        exs.putAll(map);
    }

    /**
     * @return Map of received exceptions.
     */
    public Map<ClusterNode, Exception> exceptions() {
        return exs;
    }
}
