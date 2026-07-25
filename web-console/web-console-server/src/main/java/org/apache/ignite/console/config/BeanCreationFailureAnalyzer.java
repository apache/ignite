

package org.apache.ignite.console.config;

import org.apache.ignite.internal.util.typedef.X;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.diagnostics.analyzer.AbstractInjectionFailureAnalyzer;

/**
 * An {@link AbstractInjectionFailureAnalyzer} that performs analysis of failures caused
 * by a {@link BeanCreationException}.
 */
public class BeanCreationFailureAnalyzer extends AbstractFailureAnalyzer<BeanCreationException> {
    /** {@inheritDoc} */
    @Override protected FailureAnalysis analyze(Throwable rootFailure, BeanCreationException cause) {
        return X.getThrowableList(cause).stream()
            .filter((throwable) -> throwable.getClass().equals(BeanCreationException.class))
            .findFirst()
            .map((e) -> new FailureAnalysis("Failed to create a bean from a bean definition", null, e))
            .orElse(null);
    }
}
