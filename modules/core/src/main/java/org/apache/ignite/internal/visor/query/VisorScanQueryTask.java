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

package org.apache.ignite.internal.visor.query;

import java.util.UUID;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorEither;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;

import static org.apache.ignite.internal.visor.query.VisorQueryUtils.scheduleScanStart;

/**
 * Task for execute SCAN query and get first page of results.
 */
@GridInternal
public class VisorScanQueryTask extends VisorOneNodeTask<VisorScanQueryTaskArg, VisorEither<VisorQueryResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorScanQueryJob job(VisorScanQueryTaskArg arg) {
        return new VisorScanQueryJob(arg, debug);
    }

    /**
     * Job for execute SCAN query and get first page of results.
     */
    private static class VisorScanQueryJob extends VisorJob<VisorScanQueryTaskArg, VisorEither<VisorQueryResult>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorScanQueryJob(VisorScanQueryTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorEither<VisorQueryResult> run(final VisorScanQueryTaskArg arg) {
            try {
                UUID nid = ignite.localNode().id();

                VisorQueryHolder holder = new VisorQueryHolder(false, null, null);

                ignite.cluster().<String, VisorQueryHolder>nodeLocalMap().put(holder.getQueryID(), holder);

                scheduleScanStart(ignite, holder, arg);

                return new VisorEither<>(new VisorQueryResult(nid, holder.getQueryID(), null, null, false, 0));
            }
            catch (Throwable e) {
                return new VisorEither<>(new VisorExceptionWrapper(e));
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorScanQueryJob.class, this);
        }
    }
}
