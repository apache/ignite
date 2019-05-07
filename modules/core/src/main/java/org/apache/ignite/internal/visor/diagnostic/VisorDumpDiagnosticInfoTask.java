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

package org.apache.ignite.internal.visor.diagnostic;

import java.io.File;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor;
import org.apache.ignite.internal.processors.diagnostic.PageHistoryDiagnoster;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Creates diagnostic info dump.
 */
@GridInternal
@GridVisorManagementTask
public class VisorDumpDiagnosticInfoTask extends VisorOneNodeTask<VisorDumpDiagnosticInfoArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorDumpDiagnosticInfoJob job(VisorDumpDiagnosticInfoArg arg) {
        return new VisorDumpDiagnosticInfoJob(arg, debug);
    }

    /**
     * Job that take diagnostic info dump.
     */
    private static class VisorDumpDiagnosticInfoJob extends VisorJob<VisorDumpDiagnosticInfoArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Diagnostic flag.
         */
        private VisorDumpDiagnosticInfoJob(VisorDumpDiagnosticInfoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorDumpDiagnosticInfoArg arg) {
            DiagnosticProcessor debug = ignite.context().diagnostic();

            if (arg.operation == VisorDumpDiagnosticInfoOperation.PAGE_HISTORY)
                dumpPageHistory(arg, debug);

            return null;
        }

        /**
         * @param arg Job arguments for dumping.
         * @param debug Diagnostic processor for execution.
         */
        private void dumpPageHistory(VisorDumpDiagnosticInfoArg arg, DiagnosticProcessor debug) {
            PageHistoryDiagnoster.DiagnosticPageBuilder builder = new PageHistoryDiagnoster.DiagnosticPageBuilder()
                .pageIds(arg.getPageIds());

            if (arg.getPathToDump() != null)
                builder.folderForDump(new File(arg.getPathToDump()));

            for (VisorDumpDiagnosticInfoArg.DumpAction action : arg.getDumpActions()) {
                DiagnosticProcessor.DiagnosticAction convertedAction = toInner(action);

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
        private DiagnosticProcessor.DiagnosticAction toInner(VisorDumpDiagnosticInfoArg.DumpAction action) {
            switch (action) {
                case PRINT_TO_LOG:
                    return DiagnosticProcessor.DiagnosticAction.PRINT_TO_LOG;
                case PRINT_TO_FILE:
                    return DiagnosticProcessor.DiagnosticAction.PRINT_TO_FILE;
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorDumpDiagnosticInfoJob.class, this);
        }
    }
}
