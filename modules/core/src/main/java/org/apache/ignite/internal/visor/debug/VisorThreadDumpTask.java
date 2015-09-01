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

package org.apache.ignite.internal.visor.debug;

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Creates thread dump.
 */
@GridInternal
public class VisorThreadDumpTask extends VisorOneNodeTask<Void, IgniteBiTuple<VisorThreadInfo[], long[]>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorDumpThreadJob job(Void arg) {
        return new VisorDumpThreadJob(arg, debug);
    }

    /**
     * Job that take thread dump on node.
     */
    private static class VisorDumpThreadJob extends VisorJob<Void, IgniteBiTuple<VisorThreadInfo[], long[]>> {
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
        @Override protected IgniteBiTuple<VisorThreadInfo[], long[]> run(Void arg) {
            ThreadMXBean mx = U.getThreadMx();

            ThreadInfo[] info = mx.dumpAllThreads(true, true);

            VisorThreadInfo[] visorInfo = new VisorThreadInfo[info.length];

            for (int i = 0; i < info.length; i++)
                visorInfo[i] = VisorThreadInfo.from(info[i]);

            return new IgniteBiTuple<>(visorInfo, mx.findDeadlockedThreads());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorDumpThreadJob.class, this);
        }
    }
}