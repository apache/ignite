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
