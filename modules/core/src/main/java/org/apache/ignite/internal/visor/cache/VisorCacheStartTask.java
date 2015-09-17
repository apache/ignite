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

package org.apache.ignite.internal.visor.cache;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.util.VisorTaskUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Task that start cache or near cache with specified configuration.
 */
@GridInternal
public class VisorCacheStartTask extends
    VisorMultiNodeTask<VisorCacheStartTask.VisorCacheStartArg, Map<UUID, IgniteException>, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheStartJob job(VisorCacheStartArg arg) {
        return new VisorCacheStartJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, IgniteException> reduce0(List<ComputeJobResult> results) throws IgniteException {
        Map<UUID, IgniteException> map = new HashMap<>();

        for (ComputeJobResult res : results)
            if (res.getException() != null)
                map.put(res.getNode().id(), res.getException());

        return map;
    }

    /**
     * Cache start arguments.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCacheStartArg implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final boolean near;

        /** */
        private final String name;

        /** */
        private final String cfg;

        /**
         * @param near {@code true} if near cache should be started.
         * @param name Name for near cache.
         * @param cfg Cache XML configuration.
         */
        public VisorCacheStartArg(boolean near, String name, String cfg) {
            this.near = near;
            this.name = name;
            this.cfg = cfg;
        }

        /**
         * @return {@code true} if near cache should be started.
         */
        public boolean near() {
            return near;
        }

        /**
         * @return Name for near cache.
         */
        public String name() {
            return name;
        }

        /**
         * @return Cache XML configuration.
         */
        public String configuration() {
            return cfg;
        }
    }

    /**
     * Job that start cache or near cache with specified configuration.
     */
    private static class VisorCacheStartJob extends VisorJob<VisorCacheStartArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Contains cache name and XML configurations of cache.
         * @param debug Debug flag.
         */
        private VisorCacheStartJob(VisorCacheStartArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorCacheStartArg arg) throws IgniteException {
            String cfg = arg.configuration();

            assert !F.isEmpty(cfg);

            try (ByteArrayInputStream bais = new ByteArrayInputStream(cfg.getBytes())) {
                if (arg.near) {
                    NearCacheConfiguration nearCfg = Ignition.loadSpringBean(bais, "nearCacheConfiguration");

                    ignite.getOrCreateNearCache(VisorTaskUtils.unescapeName(arg.name()), nearCfg);
                }
                else {
                    CacheConfiguration cacheCfg = Ignition.loadSpringBean(bais, "cacheConfiguration");

                    ignite.getOrCreateCache(cacheCfg);
                }
            }
            catch (IOException e) {
                throw new  IgniteException(e);
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheStartJob.class, this);
        }
    }
}
