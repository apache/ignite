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

package org.gridgain.grid.kernal.visor.misc;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Ack task to run on node.
 */
@GridInternal
public class VisorAckTask extends VisorMultiNodeTask<String, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorAckJob job(String arg) {
        return new VisorAckJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Void reduce0(List<ComputeJobResult> results) throws IgniteCheckedException {
        return null;
    }

    /**
     * Ack job to run on node.
     */
    private static class VisorAckJob extends VisorJob<String, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Message to ack in node console.
         * @param debug Debug flag.
         */
        private VisorAckJob(String arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(String arg) throws IgniteCheckedException {
            System.out.println("<visor>: ack: " + (arg == null ? g.localNode().id() : arg));

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorAckJob.class, this);
        }
    }
}
