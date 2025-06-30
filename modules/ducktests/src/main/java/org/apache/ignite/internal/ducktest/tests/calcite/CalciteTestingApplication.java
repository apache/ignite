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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.processors.query.calcite.integration.StdSqlOperatorsTest;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

/**
 * Calcite engine tests.
 */
public class CalciteTestingApplication extends IgniteAwareApplication {
    public static void main(String[] args) {
        CalciteTestingApplication app = new CalciteTestingApplication();

        ObjectMapper mapper = new ObjectMapper();

        JsonNode emptyConfig = mapper.createObjectNode();

        app.run(emptyConfig);

    }

    /** Run StdSqlOperatorsTest tests. */
    @Override
    public void run(JsonNode jsonNode) {
        markInitialized();

        Result junitResult = JUnitCore.runClasses(StdSqlOperatorsTest.class);

        if (!junitResult.wasSuccessful()) {
            String msg = String.valueOf(junitResult.getFailures().get(0));
            throw new RuntimeException(msg);
        }
        markFinished();
    }
}
