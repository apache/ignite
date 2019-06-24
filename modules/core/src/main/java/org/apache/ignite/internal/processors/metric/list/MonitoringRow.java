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

package org.apache.ignite.internal.processors.metric.list;

import org.apache.ignite.cluster.ClusterNode;

/**
 * Monitoring list row.
 * Idenitified by the instance of {@code Id}.
 * Created in response of activity from some {@link #sessionId()}.
 */
public interface MonitoringRow<Id> {
    /**
     * @return Idenitifier of entity described by this row.
     */
    public Id id();

    /**
     * Return idenitifier of activity that created this row.
     * Such activity can be one(but not limited):
     * <ul>
     *     <li>{@link ClusterNode#consistentId()}</li>
     *     <li>Thin client address.</li>
     *     <li>User ID.</li>
     * </ul>
     *
     * The choice of particularly session id should be made by the list contributor.
     * In the future all subsystem should be using the same sessionId for all Ignite entities.
     *
     * @return Idenitifier of activity that created this row.
     */
    public String sessionId();
}
