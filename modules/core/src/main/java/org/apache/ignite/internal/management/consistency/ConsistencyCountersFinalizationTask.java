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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 *
 */
public class ConsistencyCountersFinalizationTask extends VisorOneNodeTask<NoArg, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, String> job(NoArg arg) {
        return new VisorConsistencyCountersFinalizationJob(arg, debug);
    }

    /**
     *
     */
    private static class VisorConsistencyCountersFinalizationJob extends VisorJob<NoArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Arguments.
         * @param debug Debug.
         */
        protected VisorConsistencyCountersFinalizationJob(NoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(NoArg arg) throws IgniteException {
            try {
                ignite.context().cache().finalizePartitionsCounters().get();

                return "Partition update counters finalized successfully.";
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}
