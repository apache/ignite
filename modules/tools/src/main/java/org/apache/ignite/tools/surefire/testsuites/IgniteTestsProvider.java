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
import org.apache.maven.surefire.common.junit4.JUnit4StackTraceWriter;
import org.apache.maven.surefire.common.junit48.JUnit48TestChecker;
import org.apache.maven.surefire.providerapi.AbstractProvider;
import org.apache.maven.surefire.providerapi.ProviderParameters;
import org.apache.maven.surefire.report.ConsoleOutputReceiver;
import org.apache.maven.surefire.report.ReporterFactory;
import org.apache.maven.surefire.report.RunListener;
import org.apache.maven.surefire.report.SimpleReportEntry;
import org.apache.maven.surefire.suite.RunResult;
import org.apache.maven.surefire.testset.TestSetFailedException;
import org.apache.maven.surefire.util.ScanResult;
import org.apache.maven.surefire.util.ScannerFilter;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import static org.apache.maven.surefire.report.ConsoleOutputCapture.startCapture;

/**
 * Goal of the provider to find unit tests that are not part of any test suite and notify user about it.
 *
 * This provider is used when command mvn:test is executed. It collect information about test classes
 * and than checks it in {@link CheckAllTestsInSuites} test class.
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

        Result junitResult = JUnitCore.runClasses(CheckAllTestsInSuites.class);

        RunResult surefireResult = new RunResult(
            junitResult.getRunCount(), 0, junitResult.getFailureCount(), 0);

        if (junitResult.getFailureCount() > 0)
            writeFailureToOutput(junitResult.getFailures().get(0));

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
                failure.getDescription().getClassName(), null,
                failure.getDescription().getMethodName(), null,
                new JUnit4StackTraceWriter(failure));

            RunListener reporter = reporterFactory.createReporter();

            startCapture((ConsoleOutputReceiver)reporter);

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
