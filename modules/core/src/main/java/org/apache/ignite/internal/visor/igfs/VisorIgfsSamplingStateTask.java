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

package org.apache.ignite.internal.visor.igfs;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Task to set IGFS instance sampling state.
 */
@GridInternal
public class VisorIgfsSamplingStateTask extends VisorOneNodeTask<IgniteBiTuple<String, Boolean>, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Job that perform parsing of IGFS profiler logs.
     */
    private static class VisorIgfsSamplingStateJob extends VisorJob<IgniteBiTuple<String, Boolean>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorIgfsSamplingStateJob(IgniteBiTuple<String, Boolean> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(IgniteBiTuple<String, Boolean> arg) {
            try {
                ((IgfsEx)ignite.fileSystem(arg.get1())).globalSampling(arg.get2());

                return null;
            }
            catch (IllegalArgumentException iae) {
                throw new IgniteException("Failed to set sampling state for IGFS: " + arg.get1(), iae);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorIgfsSamplingStateJob.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorIgfsSamplingStateJob job(IgniteBiTuple<String, Boolean> arg) {
        return new VisorIgfsSamplingStateJob(arg, debug);
    }
}