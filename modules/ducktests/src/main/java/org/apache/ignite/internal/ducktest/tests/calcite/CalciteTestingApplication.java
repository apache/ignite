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
package org.apache.ignite.internal.ducktest.tests.calcite;

import org.apache.ignite.internal.processors.query.calcite.integration.StdSqlOperatorsTest;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

/**
 * Calcite engine tests
 */
public class CalciteTestingApplication {

    public static void main(String[] args) throws Exception {
        Result junitResult = JUnitCore.runClasses(StdSqlOperatorsTest.class);

        if (!junitResult.wasSuccessful()) {
            String errMessage = String.valueOf(junitResult.getFailures().get(0));
            throw new RuntimeException(errMessage);
        }

        System.out.println("IGNITE_APPLICATION_FINISHED");
    }
}
