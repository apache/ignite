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

package org.apache.ignite.internal.visor.encryption;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * View/change cache group re-encryption rate limit.
 */
public class VisorReencryptionRateTask extends VisorMultiNodeTask<Double, Map<UUID, Object>, Double> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<Double, Double> job(Double arg) {
        return new VisorStartReencryptionJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, Object> reduce0(List<ComputeJobResult> results) {
        Map<UUID, Object> errs = new HashMap<>();

        for (ComputeJobResult res : results)
            errs.put(res.getNode().id(), res.getException() != null ? res.getException() : res.getData());

        return errs;
    }

    /** The job for getting the master key name. */
    private static class VisorStartReencryptionJob extends VisorJob<Double, Double> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorStartReencryptionJob(Double arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Double run(Double rate) throws IgniteException {
            double prevRate = ignite.context().encryption().getReencryptionRate();

            if (rate != null)
                ignite.context().encryption().setReencryptionRate(rate);

            return prevRate;
        }
    }
}
