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

package org.apache.ignite.failure;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor;
import org.apache.ignite.internal.processors.diagnostic.PageHistoryDiagnoster;
import org.apache.ignite.internal.util.typedef.X;

import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DiagnosticAction.PRINT_TO_FILE;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DiagnosticAction.PRINT_TO_LOG;

/**
 * Diagnostic failer handler work as proxy and perform some diagnostic actions.
 */
public class DiagnosticFailureHandler extends AbstractFailureHandler {
    /** */
    private final AbstractFailureHandler delegate;

    /**
     * @param delegate Delegate failer hanlder.
     */
    public DiagnosticFailureHandler(AbstractFailureHandler delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
        // If we have some corruption in data structure,
        // we should scan WAL and print to log and save to file all pages related to corruption for
        // future investigation.
        if (X.hasCause(failureCtx.error(), CorruptedTreeException.class)) {
            CorruptedTreeException corruptedTreeException = X.cause(failureCtx.error(), CorruptedTreeException.class);

            try {
                DiagnosticProcessor diagnosticProc = ((IgniteEx)ignite).context().diagnostic();

                diagnosticProc.dumpPageHistory(
                    new PageHistoryDiagnoster.DiagnosticPageBuilder()
                        .pageIds(corruptedTreeException.pages())
                        .addAction(PRINT_TO_LOG)
                        .addAction(PRINT_TO_FILE)
                );
            }
            catch (IgniteCheckedException e) {
                ignite.log().error("Failed to dump diagnostic info on tree corruption.", e);
            }
        }

        return delegate.handle(ignite, failureCtx);
    }
}
