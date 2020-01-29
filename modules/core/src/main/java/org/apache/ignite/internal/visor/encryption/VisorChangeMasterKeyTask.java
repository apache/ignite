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

import org.apache.ignite.IgniteEncryption;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.commandline.encryption.EncryptionCommand;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * The task for changing the master key.
 *
 * @see EncryptionCommand
 * @see IgniteEncryption#changeMasterKey(String)
 */
@GridInternal
public class VisorChangeMasterKeyTask extends VisorOneNodeTask<String, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<String, String> job(String arg) {
        return new VisorChangeMasterKeyJob(arg, debug);
    }

    /** The job for changing the master key. */
    private static class VisorChangeMasterKeyJob extends VisorJob<String, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param masterKeyName Master key name.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorChangeMasterKeyJob(String masterKeyName, boolean debug) {
            super(masterKeyName, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(String masterKeyName) throws IgniteException {
            ignite.encryption().changeMasterKey(masterKeyName).get();

            return "The master key changed.";
        }
    }
}
