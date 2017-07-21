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

package org.apache.ignite.internal.visor.misc;

import java.util.List;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Ack task to run on node.
 */
@GridInternal
public class VisorAckTask extends VisorMultiNodeTask<VisorAckTaskArg, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorAckJob job(VisorAckTaskArg arg) {
        return new VisorAckJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Void reduce0(List<ComputeJobResult> results) {
        return null;
    }

    /**
     * Ack job to run on node.
     */
    private static class VisorAckJob extends VisorJob<VisorAckTaskArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Message to ack in node console.
         * @param debug Debug flag.
         */
        private VisorAckJob(VisorAckTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorAckTaskArg arg) {
            System.out.println("<visor>: ack: " + (arg.getMessage() == null ? ignite.localNode().id() : arg.getMessage()));

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorAckJob.class, this);
        }
    }
}
