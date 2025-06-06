

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
