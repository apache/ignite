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

package org.apache.ignite.testframework.test;

import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridRegexpLogger;
import org.junit.BeforeClass;

/**
 *
 */
public class GridRegexpCategoryDelegateLoggerTest extends GridRegexpDelegateLoggerTest {
    /** {@inheritDoc} */
    @BeforeClass
    public static void beforeClass() {
        useDelegateLog = true;

        delegateOutput = new StringBuffer();

        log = new GridRegexpLogger(new NullLogger() {
            @Override public void debug(String msg) {
                delegateOutput.append(msg);
            }

            @Override public void warning(String msg) {
                delegateOutput.append(msg);
            }

            @Override public void info(String msg) {
                delegateOutput.append(msg);
            }

            @Override public void trace(String msg) {
                delegateOutput.append(msg);
            }

            @Override public void error(String msg) {
                delegateOutput.append(msg);
            }
        }).getLogger("category");
    }
}
