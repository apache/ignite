/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

/**
 * Task to set IGFS instance sampling state.
 */
@GridInternal
public class VisorIgfsSamplingStateTask extends VisorOneNodeTask<VisorIgfsSamplingStateTaskArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Job that perform parsing of IGFS profiler logs.
     */
    private static class VisorIgfsSamplingStateJob extends VisorJob<VisorIgfsSamplingStateTaskArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorIgfsSamplingStateJob(VisorIgfsSamplingStateTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorIgfsSamplingStateTaskArg arg) {
            try {
                ((IgfsEx)ignite.fileSystem(arg.getName())).globalSampling(arg.isEnabled());

                return null;
            }
            catch (IllegalArgumentException iae) {
                throw new IgniteException("Failed to set sampling state for IGFS: " + arg.getName(), iae);
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
    @Override protected VisorIgfsSamplingStateJob job(VisorIgfsSamplingStateTaskArg arg) {
        return new VisorIgfsSamplingStateJob(arg, debug);
    }
}
