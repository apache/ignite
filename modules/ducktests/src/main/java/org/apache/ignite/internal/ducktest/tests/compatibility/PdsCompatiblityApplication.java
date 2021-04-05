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

package org.apache.ignite.internal.ducktest.tests.compatibility;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Simple application that have 2 options
 * 1 - load some predefined data to cache
 * 2 - check if we have that predifined data in that cache
 */
public class PdsCompatiblityApplication extends IgniteAwareApplication {
    /** Predefined test data. */
    private static List<String> users = Arrays.asList("John Connor", "Sarah Connor", "Kyle Reese");

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws IgniteCheckedException {

        String operation = jsonNode.get("operation").asText();
        final String cacheName = "users";

        markInitialized();

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cacheName);

        log.info("Input data: " + jsonNode.toString());

        switch (operation) {
            case "load":
                for (int i = 0; i < users.size(); i++) {
                    cache.put(i, users.get(i));
                }
                break;

            case "check":
                for (int i = 0; i < users.size(); i++) {
                    assert cache.get(i).equals(users.get(i));
                }
                break;

            default:
                throw new IgniteCheckedException("Unknown operation: " + operation + ".");
        }

        markFinished();
    }
}
