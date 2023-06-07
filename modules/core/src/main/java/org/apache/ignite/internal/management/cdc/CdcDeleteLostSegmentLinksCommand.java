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

package org.apache.ignite.internal.management.cdc;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cdc.VisorCdcDeleteLostSegmentsTask;
import org.apache.ignite.lang.IgniteExperimental;

import static org.apache.ignite.internal.management.api.CommandUtils.servers;

/**
 * Command to delete lost segment links.
 */
@IgniteExperimental
public class CdcDeleteLostSegmentLinksCommand implements ComputeCommand<CdcDeleteLostSegmentLinksCommandArg, Void> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Delete lost segment CDC links";
    }

    /** {@inheritDoc} */
    @Override public Class<CdcDeleteLostSegmentLinksCommandArg> argClass() {
        return CdcDeleteLostSegmentLinksCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorCdcDeleteLostSegmentsTask> taskClass() {
        return VisorCdcDeleteLostSegmentsTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(
        Map<UUID, T3<Boolean, Object, Long>> nodes,
        CdcDeleteLostSegmentLinksCommandArg arg
    ) {
        return arg.nodeId() != null
            ? Collections.singleton(arg.nodeId())
            : servers(nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(CdcDeleteLostSegmentLinksCommandArg arg, Void res, Consumer<String> printer) {
        printer.accept("Lost segment CDC links successfully removed.");
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(CdcDeleteLostSegmentLinksCommandArg arg) {
        return "Warning: The command will fix WAL segments gap in case CDC link creation was stopped by distributed " +
            "property or excess of maximum CDC directory size. Gap will be fixed by deletion of WAL segment links" +
            "previous to the last gap." + U.nl() +
            "All changes in deleted segment links will be lost!" + U.nl() +
            "Make sure you need to sync data before restarting the CDC application. You can synchronize caches " +
            "using snapshot or other methods.";
    }
}
