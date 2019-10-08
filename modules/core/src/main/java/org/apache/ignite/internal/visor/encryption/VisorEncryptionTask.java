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

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task for encryption features.
 */
@GridInternal
public class VisorEncryptionTask extends VisorOneNodeTask<VisorEncryptionArgs, VisorEncryptionTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorEncryptionArgs, VisorEncryptionTaskResult> job(VisorEncryptionArgs arg) {
        return new VisorEncryptionJob(arg, debug);
    }

    /**
     * Job for encryption features.
     */
    private static class VisorEncryptionJob extends VisorJob<VisorEncryptionArgs, VisorEncryptionTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg   Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorEncryptionJob(@Nullable VisorEncryptionArgs arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorEncryptionTaskResult run(@Nullable VisorEncryptionArgs arg) throws IgniteException {
            switch (arg.getCmd()) {
                case GET_MASTER_KEY:
                    String masterKeyId = ignite.encryption().getMasterKeyId();

                    return new VisorEncryptionTaskResult(masterKeyId);

                case CHANGE_MASTER_KEY:
                    ignite.encryption().changeMasterKey(arg.getMasterKeyId());

                    return new VisorEncryptionTaskResult("Master key changed.");

                default:
                    throw new IllegalArgumentException("Unknown encryption subcommand: " + arg.getCmd());
            }
        }
    }
}
