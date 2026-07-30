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

package org.apache.ignite.internal.processors.continuous;

import java.util.List;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/** */
public class ContinuousRoutinesJoiningNodeDiscoveryData implements Message {
    /** */
    @Order(0)
    List<ContinuousRoutineInfo> startedRoutines;

    /** Empty constructor for serialization purposes. */
    public ContinuousRoutinesJoiningNodeDiscoveryData() {
        // No-op.
    }

    /** @param startedRoutines Routines registered on nodes, to be started in cluster. */
    ContinuousRoutinesJoiningNodeDiscoveryData(List<ContinuousRoutineInfo> startedRoutines) {
        this.startedRoutines = startedRoutines;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ContinuousRoutinesJoiningNodeDiscoveryData.class, this);
    }
}
