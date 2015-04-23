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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.lang.*;

import java.io.*;
import java.nio.charset.*;

/**
 * Task that stop specified caches on specified node.
 */
@GridInternal
public class VisorCacheStartNearTask extends VisorOneNodeTask<IgniteBiTuple<String, String>, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheStartJob job(IgniteBiTuple<String, String> arg) {
        return new VisorCacheStartJob(arg, debug);
    }

    /**
     * Job that stop specified caches.
     */
    private static class VisorCacheStartJob extends VisorJob<IgniteBiTuple<String, String>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Contains XML configurations of cache and near cache tuple.
         * @param debug Debug flag.
         */
        private VisorCacheStartJob(IgniteBiTuple<String, String> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(IgniteBiTuple<String, String> arg) {
            assert arg.get1() != null;
            assert arg.get2() != null;

            NearCacheConfiguration nearCfg = Ignition.loadSpringBean(
                new ByteArrayInputStream(arg.get2().getBytes(StandardCharsets.UTF_8)), "nearCacheConfiguration");

            ignite.createNearCache(arg.get1(), nearCfg);

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheStartJob.class, this);
        }
    }
}
