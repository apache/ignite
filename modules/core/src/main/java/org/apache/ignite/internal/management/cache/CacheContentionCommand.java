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

package org.apache.ignite.internal.management.cache;

import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;

/** Prints info about contended keys (the keys concurrently locked from multiple transactions). */
public class CacheContentionCommand implements ComputeCommand<CacheContentionCommandArg, ContentionTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Show the keys that are point of contention for multiple transactions";
    }

    /** {@inheritDoc} */
    @Override public Class<CacheContentionCommandArg> argClass() {
        return CacheContentionCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<ContentionTask> taskClass() {
        return ContentionTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientNode> nodes(Collection<GridClientNode> nodes, CacheContentionCommandArg arg) {
        return CommandUtils.nodeOrAll(arg.nodeId(), nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(CacheContentionCommandArg arg, ContentionTaskResult res, Consumer<String> printer) {
        CommandUtils.printErrors(res.exceptions(), "Contention check failed on nodes:", printer);

        for (ContentionInfo info : res.getInfos())
            info.print();
    }
}
