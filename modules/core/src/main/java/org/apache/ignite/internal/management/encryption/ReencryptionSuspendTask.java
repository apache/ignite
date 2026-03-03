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

package org.apache.ignite.internal.management.encryption;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.jetbrains.annotations.Nullable;

/**
 * Suspend re-encryption of the cache group.
 */
@GridInternal
public class ReencryptionSuspendTask extends CacheGroupEncryptionTask<Boolean> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<EncryptionCacheGroupArg, SingleFieldDto<Boolean>> job(
        EncryptionCacheGroupArg arg) {
        return new ReencryptionSuspendJob(arg, debug);
    }

    /** The job to suspend re-encryption of the cache group. */
    private static class ReencryptionSuspendJob extends ReencryptionBaseJob<Boolean> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected ReencryptionSuspendJob(@Nullable EncryptionCacheGroupArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected SingleFieldDto<Boolean> run0(CacheGroupContext grp) throws IgniteCheckedException {
            ReencryptionSuspendResumeJobResult res = new ReencryptionSuspendResumeJobResult();

            res.val = ignite.context().encryption().suspendReencryption(grp.groupId());

            return res;
        }
    }

    /** */
    public static class ReencryptionSuspendResumeJobResult extends IgniteDataTransferObject implements SingleFieldDto<Boolean> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        @Order(0)
        Boolean val;

        /** {@inheritDoc} */
        @Override public Boolean value() {
            return val;
        }
    }
}
