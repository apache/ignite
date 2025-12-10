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

package org.apache.ignite.tools.surefire.testsuites;

import java.util.Collections;
import org.apache.maven.surefire.api.provider.AbstractProvider;
import org.apache.maven.surefire.api.provider.ProviderParameters;
import org.apache.maven.surefire.api.report.OutputReportEntry;
import org.apache.maven.surefire.api.report.ReporterFactory;
import org.apache.maven.surefire.api.report.RunListener;
import org.apache.maven.surefire.api.report.SimpleReportEntry;
import org.apache.maven.surefire.api.report.TestOutputReceiver;
import org.apache.maven.surefire.api.suite.RunResult;
import org.apache.maven.surefire.api.testset.TestSetFailedException;
import org.apache.maven.surefire.api.util.ScanResult;
import org.apache.maven.surefire.api.util.ScannerFilter;
import org.apache.maven.surefire.common.junit48.JUnit48TestChecker;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.junit.platform.launcher.listeners.TestExecutionSummary.Failure;

import static org.apache.maven.surefire.api.report.ConsoleOutputCapture.startCapture;
import static org.apache.maven.surefire.api.report.RunMode.NORMAL_RUN;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;


/**
 * Goal of the provider to find unit tests that are not part of any test suite and notify user about it.
 *
 * This provider is used when command mvn:test is executed. It collects information about test classes
 * and then checks it in {@link CheckAllTestsInSuites} test class.
 */
public class IgniteTestsProvider extends AbstractProvider {
    /** Test class loader. */
    private final ClassLoader testClsLdr;

    /** */
    private final ScanResult scanResult;

    /** */
    private final ReporterFactory reporterFactory;

    /** @param bootParams Provider parameters. */
    public IgniteTestsProvider(ProviderParameters bootParams) {
        testClsLdr = bootParams.getTestClassLoader();
        scanResult = bootParams.getScanResult();
        reporterFactory = bootParams.getReporterFactory();
    }

    /** {@inheritDoc} */
    @Override public RunResult invoke(Object forkTestSet) throws TestSetFailedException {
        ScannerFilter scannerFilter = new IgniteTestFilter(testClsLdr);

        CheckAllTestsInSuites.testClasses = scanResult.applyFilter(scannerFilter, testClsLdr);

        LauncherDiscoveryRequest request =
                LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass(CheckAllTestsInSuites.class))
                .build();

        final Launcher launcher = LauncherFactory.create();
        final SummaryGeneratingListener listener = new SummaryGeneratingListener();

        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        TestExecutionSummary summary = listener.getSummary();

        RunResult surefireResult = new RunResult(
                (int)summary.getTestsStartedCount(), 0, (int)summary.getTestsFailedCount(), 0);

        if (!summary.getFailures().isEmpty())
            writeFailureToOutput(summary.getFailures().get(0));

        return surefireResult;
    }

    /** Override default junit test checker to handle Ignite specific classes. */
    private static class IgniteTestFilter extends JUnit48TestChecker {
        /** */
        public IgniteTestFilter(ClassLoader testClsLdr) {
            super(testClsLdr);
        }

        /** {@inheritDoc} */
        @Override public boolean accept(Class aCls) {
            boolean isValidJunitTestCls = super.accept(aCls);

            if (!isValidJunitTestCls && shouldCheckSuperClass(aCls))
                return super.accept(aCls.getSuperclass());

            return isValidJunitTestCls;
        }

        /**
         * We should check super class of {@code aCls} as there are cases when classes just extend
         * real test class by overriding some non-test methods (e.g. #getConfiguration).
         */
        private boolean shouldCheckSuperClass(Class<?> aCls) {
            Class<?> superCls = aCls.getSuperclass();

            return superCls != null && !superCls.equals(Object.class);
        }
    }

    /** Make info about non-suited test visible to user. */
    private void writeFailureToOutput(Failure failure) throws TestSetFailedException {
        try {
            SimpleReportEntry report = SimpleReportEntry.withException(
                NORMAL_RUN, 0L,
                failure.getTestIdentifier().getType().getDeclaringClass().getName(), null,
                failure.getTestIdentifier().getDisplayName(), null,
                new JUnitStackTraceWriter(failure));

            RunListener reporter = reporterFactory.createTestReportListener();

            startCapture((TestOutputReceiver<OutputReportEntry>)reporter);

            reporter.testFailed(report);
            reporter.testSetCompleted(report);
        }
        catch (Exception e) {
            throw new TestSetFailedException("Failed to dump exception to stdout");
        }
    }

    /** {@inheritDoc} */
    @Override public Iterable<Class<?>> getSuites() {
        return Collections.singletonList(CheckAllTestsInSuites.class);
    }
}
