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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task to control the process of re-encryption of the cache group.
 */
public class VisorGroupReencryptionTask
    extends VisorMultiNodeTask<VisorGroupReencryptionTaskArg, Map<UUID, Object>, Object>
{
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorGroupReencryptionTaskArg, Object> job(VisorGroupReencryptionTaskArg arg) {
        return new VisorStartReencryptionJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, Object> reduce0(List<ComputeJobResult> results) {
        Map<UUID, Object> resMap = new HashMap<>();

        for (ComputeJobResult res : results)
            resMap.put(res.getNode().id(), res.getException() != null ? res.getException() : res.getData());

        return resMap;
    }

    /** The job for getting the master key name. */
    private static class VisorStartReencryptionJob extends VisorJob<VisorGroupReencryptionTaskArg, Object> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorStartReencryptionJob(VisorGroupReencryptionTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Object run(VisorGroupReencryptionTaskArg arg) throws IgniteException {
            String grpName = arg.groupName();
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

            GridEncryptionManager encMgr = ignite.context().encryption();
            int grpId = grp.groupId();

            try {
                switch (arg.type()) {
                    case STATUS:
                        return encMgr.getBytesLeftForReencryption(grpId);

                    case SUSPEND:
                        return encMgr.reencryptionFuture(grpId).cancel();

                    case RESUME:
                        if (!encMgr.reencryptionFuture(grpId).isDone())
                            return false;

                        encMgr.resumeReencryption(grpId);

                        return true;

                    default:
                        throw new UnsupportedOperationException("Not implemented task action: " + arg.type());
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}
