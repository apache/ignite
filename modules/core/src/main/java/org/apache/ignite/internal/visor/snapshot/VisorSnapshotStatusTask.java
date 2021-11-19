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

package org.apache.ignite.internal.visor.snapshot;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMXBeanImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Task that collects nodes that have snapshot operation in progress.
 */
@GridInternal
public class VisorSnapshotStatusTask extends VisorOneNodeTask<Void, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<Void, String> job(Void arg) {
        return new VisorSnapshotStatusJob(arg, debug);
    }

    /** */
    private static class VisorSnapshotStatusJob extends VisorJob<Void, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotStatusJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(Void arg) throws IgniteException {
            Set<Object> ids = new SnapshotMXBeanImpl(ignite.context()).statusSnapshot().entrySet()
                    .stream()
                    .filter(Map.Entry::getValue)
                    .map(Map.Entry::getKey).collect(Collectors.toSet());

            if (ids.isEmpty())
                return "No snapshot operations.";

            StringBuilder sb = new StringBuilder("Snapshot operation in progress on nodes with Consistent ID:");

            ids.stream()
                    .map(String::valueOf)
                    .forEach(s -> sb.append(IgniteUtils.nl()).append(s));

            return sb.toString();
        }
    }
}
