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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Visor encrypted cache group multinode task.
 *
 * @param <T> The type of the task result.
 */
public abstract class CacheGroupEncryptionTask<T> extends VisorMultiNodeTask<EncryptionCacheGroupArg,
    CacheGroupEncryptionTaskResult<T>, CacheGroupEncryptionTask.SingleFieldDto<T>> {
    /** {@inheritDoc} */
    @Nullable @Override protected CacheGroupEncryptionTaskResult<T> reduce0(List<ComputeJobResult> results) {
        Map<UUID, T> jobResults = new HashMap<>();
        Map<UUID, IgniteException> exceptions = new HashMap<>();

        for (ComputeJobResult res : results) {
            UUID nodeId = res.getNode().id();

            if (res.getException() != null) {
                exceptions.put(nodeId, res.getException());

                continue;
            }

            SingleFieldDto<T> dtoRes = res.getData();

            jobResults.put(nodeId, dtoRes.value());
        }

        return new CacheGroupEncryptionTaskResult<>(jobResults, exceptions);
    }

    /** */
    public abstract static class SingleFieldDto<T> extends IgniteDataTransferObject {
        /** Object value. */
        private T val;

        /**
          * @return Object value.
         */
        protected T value() {
            return val;
        }

        /**
         * @param val Data object.
         * @return {@code this} for chaining.
         */
        protected SingleFieldDto<T> value(T val) {
            this.val = val;

            return this;
        }
    }

    /**
     * @param <T> Type of job result.
     */
    protected abstract static class ReencryptionBaseJob<T>
        extends VisorJob<EncryptionCacheGroupArg, SingleFieldDto<T>> {
        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected ReencryptionBaseJob(@Nullable EncryptionCacheGroupArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected SingleFieldDto<T> run(EncryptionCacheGroupArg arg) throws IgniteException {
            try {
                String grpName = arg.cacheGroupName();
                CacheGroupContext grp = ignite.context().cache().cacheGroup(CU.cacheId(grpName));

                if (grp == null) {
                    IgniteInternalCache<Object, Object> cache = ignite.context().cache().cache(grpName);

                    if (cache == null)
                        throw new IgniteException("Cache group " + grpName + " not found.");

                    grp = cache.context().group();

                    if (grp.sharedGroup()) {
                        throw new IgniteException("Cache or group \"" + grpName + "\" is a part of group \"" +
                            grp.name() + "\". Provide group name instead of cache name for shared groups.");
                    }
                }

                return run0(grp);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * Executes internal logic of the job.
         *
         * @param grp Cache group.
         * @return Result.
         * @throws IgniteCheckedException In case of error.
         */
        protected abstract SingleFieldDto<T> run0(CacheGroupContext grp) throws IgniteCheckedException;
    }
}
