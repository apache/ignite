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

package org.apache.ignite.internal;

import java.util.List;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class TestNotManagementVisorMultiNodeTask extends VisorMultiNodeTask<VisorTaskArgument, Object, Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorNotManagementMultiNodeJob job(VisorTaskArgument arg) {
        return new VisorNotManagementMultiNodeJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Object reduce0(List<ComputeJobResult> results) {
        return null;
    }

    /**
     * Not management multi node visor job.
     */
    private static class VisorNotManagementMultiNodeJob extends VisorJob<VisorTaskArgument, Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug flag.
         */
        protected VisorNotManagementMultiNodeJob(VisorTaskArgument arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Object run(VisorTaskArgument arg) {
            return null;
        }
    }
}
