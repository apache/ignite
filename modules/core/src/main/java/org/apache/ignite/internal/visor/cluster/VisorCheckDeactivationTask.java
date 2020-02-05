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

package org.apache.ignite.internal.visor.cluster;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Task for checking if deactiation is safe. Returns false when deactivation can lead to data loss.
 */
@GridInternal
public class VisorCheckDeactivationTask extends VisorOneNodeTask<Void, Boolean> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<Void, Boolean> job(Void arg) {
        return new VisorCheckDeactivationJob(arg, debug);
    }

    /** The task for checking if deactiation is safe. Returns false if deactivation can lead to data loss. */
    private static class VisorCheckDeactivationJob extends VisorJob<Void, Boolean> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        protected VisorCheckDeactivationJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Boolean run(Void arg) throws IgniteException {
            return ignite.context().state().isDeactivationSafe();
        }
    }
}
