/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.event;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.plugin.security.SecurityPermission.EVENTS_DISABLE;
import static org.apache.ignite.plugin.security.SecurityPermission.EVENTS_ENABLE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;

/**
 * Enable/disable events task.
 */
@GridInternal
public class EventManagementTask extends VisorMultiNodeTask<EventCommandArg, Void, Void> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<EventCommandArg, Void> job(EventCommandArg arg) {
        return new EventManagementJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Void reduce0(List<ComputeJobResult> results) {
        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                throw res.getException();
        }

        return null;
    }

    /** The job for enable/disable events. */
    private static class EventManagementJob extends VisorJob<EventCommandArg, Void> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected EventManagementJob(EventCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(EventCommandArg arg) throws IgniteException {
            Map<Integer, String> evtToName = U.gridEventNames();

            Map<String, Integer> nameToEvt = evtToName.entrySet().stream()
                .collect(Collectors.toMap(e -> "EVT_" + e.getValue(), Map.Entry::getKey));

            int[] evtTypes = new int[arg.events.length];

            for (int i = 0; i < arg.events.length; i++) {
                String evtName = arg.events[i];

                if (!nameToEvt.containsKey(evtName))
                    throw new IgniteException("Failed to find event by name [evt=" + evtName + ']');

                evtTypes[i] = nameToEvt.get(evtName);
            }

            if (arg instanceof EventEnableCommand.EventEnableCommandArg)
                ignite.context().event().enableEvents(evtTypes);
            else
                ignite.context().event().disableEvents(evtTypes);

            return null;
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet requiredPermissions() {
            return systemPermissions(
                argument(0) instanceof EventEnableCommand.EventEnableCommandArg ? EVENTS_ENABLE : EVENTS_DISABLE
            );
        }
    }
}
