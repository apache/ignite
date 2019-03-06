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

package org.apache.ignite.internal.visor.debug;

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Creates thread dump.
 */
@GridInternal
@GridVisorManagementTask
public class VisorThreadDumpTask extends VisorOneNodeTask<Void, VisorThreadDumpTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorDumpThreadJob job(Void arg) {
        return new VisorDumpThreadJob(arg, debug);
    }

    /**
     * Job that take thread dump on node.
     */
    private static class VisorDumpThreadJob extends VisorJob<Void, VisorThreadDumpTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorDumpThreadJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorThreadDumpTaskResult run(Void arg) {
            ThreadMXBean mx = U.getThreadMx();

            ThreadInfo[] info = mx.dumpAllThreads(true, true);

            VisorThreadInfo[] visorInfo = new VisorThreadInfo[info.length];

            for (int i = 0; i < info.length; i++)
                visorInfo[i] = new VisorThreadInfo(info[i]);

            return new VisorThreadDumpTaskResult(visorInfo, mx.findDeadlockedThreads());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorDumpThreadJob.class, this);
        }
    }
}
