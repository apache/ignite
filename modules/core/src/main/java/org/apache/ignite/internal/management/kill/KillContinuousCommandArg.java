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

package org.apache.ignite.internal.management.kill;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Positional;

/** */
public class KillContinuousCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Order(value = 0)
    @Positional
    @Argument(description = "Originating node id")
    UUID originNodeId;

    /** */
    @Order(value = 1)
    @Positional
    @Argument(description = "Routine identifier")
    UUID routineId;

    /** */
    public UUID originNodeId() {
        return originNodeId;
    }

    /** */
    public void originNodeId(UUID originNodeId) {
        this.originNodeId = originNodeId;
    }

    /** */
    public UUID routineId() {
        return routineId;
    }

    /** */
    public void routineId(UUID routineId) {
        this.routineId = routineId;
    }
}
