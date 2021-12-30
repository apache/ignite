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

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMXBeanImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task that collects information about a snapshot operation on nodes.
 */
@GridInternal
public class VisorSnapshotStatusTask extends VisorOneNodeTask<String, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<String, String> job(@Nullable String arg) {
        return new VisorSnapshotStatusJob(arg, debug);
    }

    /** */
    private static class VisorSnapshotStatusJob extends VisorJob<String, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotStatusJob(@Nullable String arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(@Nullable String arg) throws IgniteException {
            Map<Object, String> statusMap = new SnapshotMXBeanImpl(ignite.context()).statusSnapshot();

            StringBuilder sb = new StringBuilder("Status of SNAPSHOT operations:").append(IgniteUtils.nl());

            statusMap.forEach((key, value) -> sb.append(key).append(" -> ").append(value).append(IgniteUtils.nl()));

            return sb.toString();
        }
    }
}
