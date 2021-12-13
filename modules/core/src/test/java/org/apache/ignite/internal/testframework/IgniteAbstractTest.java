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

import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Ignite base test class.
 */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class IgniteAbstractTest extends BaseIgniteAbstractTest {
    /** Work directory. */
    protected Path workDir;

    /**
     * Invokes before the test will start.
     *
     * @param testInfo Test information oject.
     * @param workDir  Work directory.
     * @throws Exception If failed.
     */
    @BeforeEach
    public void setup(TestInfo testInfo, @WorkDirectory Path workDir) throws Exception {
        setupBase(testInfo, workDir);

        this.workDir = workDir;
    }

    /**
     * Invokes after the test has finished.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @AfterEach
    public void tearDown(TestInfo testInfo) throws Exception {
        tearDownBase(testInfo);
    }
}
