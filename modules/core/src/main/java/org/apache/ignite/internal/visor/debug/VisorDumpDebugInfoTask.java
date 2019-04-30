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

import java.io.File;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.maintain.DebugProcessor;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Creates debug info dump.
 */
@GridInternal
@GridVisorManagementTask
public class VisorDumpDebugInfoTask extends VisorOneNodeTask<VisorDumpDebugInfoArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorDumpDebugInfoJob job(VisorDumpDebugInfoArg arg) {
        return new VisorDumpDebugInfoJob(arg, debug);
    }

    /**
     * Job that take debug info dump.
     */
    private static class VisorDumpDebugInfoJob extends VisorJob<VisorDumpDebugInfoArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorDumpDebugInfoJob(VisorDumpDebugInfoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorDumpDebugInfoArg arg) {
            DebugProcessor debug = ignite.context().debug();

            if (arg.operation == VisorDumpDebugInfoOperation.PAGE_HISTORY)
                dumpPageHistory(arg, debug);

            return null;
        }

        /**
         * @param arg Job arguments for dumping.
         * @param debug Debug processor for execution.
         */
        private void dumpPageHistory(VisorDumpDebugInfoArg arg, DebugProcessor debug) {
            DebugProcessor.DebugPageBuilder builder = new DebugProcessor.DebugPageBuilder()
                .pageIds(arg.getPageIds());

            if (arg.getPathToDump() != null)
                builder.fileOrFolderForDump(new File(arg.getPathToDump()));

            for (VisorDumpDebugInfoArg.DumpAction action : arg.getDumpActions()) {
                DebugProcessor.DebugAction convertedAction = toInner(action);

                if (convertedAction != null)
                    builder.addAction(convertedAction);
            }

            try {
                debug.dumpPageHistory(builder);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * Converting visor action to inner debug action.
         *
         * @param action Action for converting.
         * @return Inner debug action.
         */
        private DebugProcessor.DebugAction toInner(VisorDumpDebugInfoArg.DumpAction action) {
            switch (action) {
                case PRINT_TO_LOG:
                    return DebugProcessor.DebugAction.PRINT_TO_LOG;
                case PRINT_TO_FILE:
                    return DebugProcessor.DebugAction.PRINT_TO_FILE;
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorDumpDebugInfoJob.class, this);
        }
    }
}
