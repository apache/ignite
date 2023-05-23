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

package org.apache.ignite.internal.management.consistency;

import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.ExperimentalCommand;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyCountersFinalizationTask;

/** */
public class ConsistencyFinalizeCommand implements
    ExperimentalCommand<NoArg, String>, ComputeCommand<NoArg, String> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Finalize partitions update counters";
    }

    /** {@inheritDoc} */
    @Override public Class<NoArg> argClass() {
        return NoArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorConsistencyCountersFinalizationTask> taskClass() {
        return VisorConsistencyCountersFinalizationTask.class;
    }
}
