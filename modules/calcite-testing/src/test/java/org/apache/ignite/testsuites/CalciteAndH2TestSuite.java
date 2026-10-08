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

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.query.calcite.integration.TimeoutIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.jdbc.JdbcCrossEngineTest;
import org.apache.ignite.internal.processors.query.calcite.thin.MultiLineQueryTest;
import org.apache.ignite.internal.processors.tx.TxWithExceptionalInterceptorTest;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Tests that require both SQL engines, Calcite and H2, on the classpath.
 */
@Suite
@SelectClasses({
    JdbcCrossEngineTest.class,
    MultiLineQueryTest.class,
    TimeoutIntegrationTest.class,
    TxWithExceptionalInterceptorTest.class,
})
public class CalciteAndH2TestSuite {
}
