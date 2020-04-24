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

package org.apache.ignite.internal.commandline.cache.check_indexes_inline_size;

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.commandline.cache.CheckIndexInlineSizes;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task for {@link CheckIndexInlineSizes} command.
 */
@GridInternal
public class CheckIndexInlineSizesTask extends VisorMultiNodeTask<Void, CheckIndexInlineSizesResult, CheckIndexInlineSizesResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<Void, CheckIndexInlineSizesResult> job(Void arg) {
        return new CheckIndexInlineSizesJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected CheckIndexInlineSizesResult reduce0(List<ComputeJobResult> results) {
        CheckIndexInlineSizesResult res = new CheckIndexInlineSizesResult();

        boolean foundEx = false;

        for (ComputeJobResult jobResult : results) {
            if (jobResult.getException() == null)
                res.merge(jobResult.getData());
            else {
                foundEx = true;

                break;
            }
        }

        if (foundEx) {
            IgniteException compoundEx = new IgniteException();

            for (ComputeJobResult jobResult : results) {
                if (jobResult.getException() != null)
                    compoundEx.addSuppressed(jobResult.getException());
            }

            throw compoundEx;
        }

        return res;
    }

    /**
     * Job for {@link CheckIndexInlineSizes} command.
     */
    private static class CheckIndexInlineSizesJob extends VisorJob<Void, CheckIndexInlineSizesResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        protected CheckIndexInlineSizesJob(@Nullable Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected CheckIndexInlineSizesResult run(@Nullable Void arg) throws IgniteException {
            Map<String, Integer> indexNameToInlineSize = ignite.context().query().secondaryIndexesInlineSize();

            CheckIndexInlineSizesResult res = new CheckIndexInlineSizesResult();

            res.addResult(ignite.localNode().id(), indexNameToInlineSize);

            return res;
        }
    }
}
