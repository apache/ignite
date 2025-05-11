/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.diagnostics.FailureAnalysisReporter;

/**
 * {@link FailureAnalysisReporter} that logs the failure analysis.
 */
public class CustomFailureAnalysisReporter implements FailureAnalysisReporter {
    /** */
    private static final Log logger = LogFactory.getLog(CustomFailureAnalysisReporter.class);

    /** {@inheritDoc} */
    @Override public void report(FailureAnalysis failureAnalysis) {
        if (logger.isErrorEnabled()) {
            Throwable e = failureAnalysis.getCause();

            logger.error("Failed to start Web Console: " + e.getMessage(), failureAnalysis.getCause());
        }
    }
}
