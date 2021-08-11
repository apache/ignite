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

package org.apache.ignite.internal.testframework;

import java.lang.reflect.Method;
import java.nio.file.Path;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tostring.SensitiveDataLoggingPolicy;
import org.apache.ignite.lang.IgniteLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.ignite.internal.util.IgniteUtils.monotonicMs;
import static org.apache.ignite.lang.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;
import static org.apache.ignite.lang.IgniteSystemProperties.getString;

/**
 * Ignite base test class.
 */
@ExtendWith({SystemPropertiesExtension.class, WorkDirectoryExtension.class})
public abstract class IgniteAbstractTest {
    /** Logger. */
    protected static IgniteLogger log;

    /** Tets start milliseconds. */
    private long testStartMs;

    /** Work directory. */
    protected Path workDir;

    /** Init test env. */
    static {
        S.setSensitiveDataLoggingPolicySupplier(() ->
            SensitiveDataLoggingPolicy.valueOf(getString(IGNITE_SENSITIVE_DATA_LOGGING, "hash").toUpperCase()));
    }

    /**
     * Invokes before the test will start.
     *
     * @param testInfo Test information oject.
     * @param workDir Work directory.
     * @throws Exception If failed.
     */
    @BeforeEach
    public void setup(TestInfo testInfo, @WorkDirectory Path workDir) throws Exception {
        log.info(">>> Starting test: {}#{}, displayName: {}, workDir: {}",
            testInfo.getTestClass().map(Class::getSimpleName).orElseGet(() -> "<null>"),
            testInfo.getTestMethod().map(Method::getName).orElseGet(() -> "<null>"),
            testInfo.getDisplayName(),
            workDir.toAbsolutePath());

        this.workDir = workDir;
        this.testStartMs = monotonicMs();
    }

    /**
     * Invokes after the test has finished.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @AfterEach
    public void tearDown(TestInfo testInfo) throws Exception {
        log.info(">>> Stopping test: {}#{}, displayName: {}, cost: {}ms.",
            testInfo.getTestClass().map(Class::getSimpleName).orElseGet(() -> "<null>"),
            testInfo.getTestMethod().map(Method::getName).orElseGet(() -> "<null>"),
            testInfo.getDisplayName(), monotonicMs() - testStartMs);
    }

    /**
     * Constructor.
     */
    @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
    protected IgniteAbstractTest() {
        log = IgniteLogger.forClass(getClass());
    }

    /**
     * @return Logger.
     */
    protected IgniteLogger logger() {
        return log;
    }
}
