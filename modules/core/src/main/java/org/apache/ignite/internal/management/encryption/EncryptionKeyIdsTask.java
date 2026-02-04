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

import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.jetbrains.annotations.Nullable;

/**
 * Get current encryption key IDs of the cache group.
 */
@GridInternal
public class EncryptionKeyIdsTask extends CacheGroupEncryptionTask<List<Integer>> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<EncryptionCacheGroupArg, SingleFieldDto<List<Integer>>> job(
        EncryptionCacheGroupArg arg) {
        return new EncryptionKeyIdsJob(arg, debug);
    }

    /** The job for get current encryption key IDs of the cache group. */
    private static class EncryptionKeyIdsJob extends ReencryptionBaseJob<List<Integer>> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected EncryptionKeyIdsJob(@Nullable EncryptionCacheGroupArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected SingleFieldDto<List<Integer>> run0(CacheGroupContext grp) {
            EncryptionKeyIdsResult res = new EncryptionKeyIdsResult();

            res.val = ignite.context().encryption().groupKeyIds(grp.groupId());

            return res;
        }
    }

    /** */
    public static class EncryptionKeyIdsResult extends IgniteDataTransferObject implements SingleFieldDto<List<Integer>> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        List<Integer> val;

        /** {@inheritDoc} */
        @Override public List<Integer> value() {
            return val;
        }
    }
}
