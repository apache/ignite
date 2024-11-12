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

package org.apache.ignite.internal.ducktest.tests.dump;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Application to control the cache dump operations (just create for now).
 */
public class DumpUtility extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jsonNode) {
        String cmd = jsonNode.get("cmd").asText();
        String dumpName = jsonNode.get("dumpName").asText();

        markInitialized();

        switch (cmd) {
            case "create":
                long startTime = System.nanoTime();

                ignite.snapshot().createDump(dumpName, null).get();

                long resultTime = System.nanoTime() - startTime;

                recordResult("DUMP_CREATE_TIME_MS", resultTime / 1_000_000);

                break;
            default:
                throw new RuntimeException("Wrong cmd parameter for the dump control utility: '" + cmd + "'");
        }

        markFinished();
    }
}
