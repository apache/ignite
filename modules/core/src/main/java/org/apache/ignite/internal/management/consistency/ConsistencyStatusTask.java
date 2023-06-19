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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.resources.LoggerResource;

/**
 *
 */
public class ConsistencyStatusTask extends AbstractConsistencyTask<NoArg, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Nothing found. */
    public static final String NOTHING_FOUND = "Consistency check/repair operations were NOT found.";

    /** Status map. */
    public static final ConcurrentHashMap<String, String> MAP = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, String> job(NoArg arg) {
        return new VisorConsistencyStatusJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected ConsistencyTaskResult reduce0(List<ComputeJobResult> results) throws IgniteException {
        ConsistencyTaskResult taskRes = super.reduce0(results);

        if (taskRes.message() == null)
            taskRes.message(NOTHING_FOUND);

        return taskRes;
    }

    /**
     *
     */
    private static class VisorConsistencyStatusJob extends VisorJob<NoArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Injected logger. */
        @LoggerResource
        protected IgniteLogger log;

        /**
         * @param arg Arguments.
         * @param debug Debug.
         */
        protected VisorConsistencyStatusJob(NoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(NoArg arg) throws IgniteException {
            if (MAP.isEmpty())
                return null;

            StringBuilder sb = new StringBuilder();

            for (Map.Entry<String, String> entry : MAP.entrySet()) {
                sb.append("\n    Job: ").append(entry.getKey()).append("\n")
                    .append("    Status: ").append(entry.getValue()).append("\n");
            }

            return sb.toString();
        }
    }
}
