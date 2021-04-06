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
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Simple application that have 2 options.
 * "load" - load some predefined data to cache.
 * "check" - check if we have that predifined data in that cache.
 */
public class PdsCompatiblityApplication extends IgniteAwareApplication {
    /** Predefined test data. */
    private static List<User> users = Arrays.asList(
            new User(0, "John Connor"),
            new User(1, "Sarah Connor"),
            new User(2, "Kyle Reese"));

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws IgniteCheckedException {
        String operation = jsonNode.get("operation").asText();

        markInitialized();

        IgniteCache<Integer, User> cache = ignite.getOrCreateCache("users");

        switch (operation) {
            case "load":
                for (int i = 0; i < users.size(); i++)
                    cache.put(i, users.get(i));

                break;

            case "check":
                for (int i = 0; i < users.size(); i++)
                    assert cache.get(i).equals(users.get(i));

                break;

            default:
                throw new IgniteCheckedException("Unknown operation: " + operation + ".");
        }

        markFinished();
    }

    /**
     * Data model class, which instances used as cache entry values.
     */
    private static class User {
        /** */
        private Integer id;

        /** */
        private String fullName;

        /**
         * @param id user id.
         * @param fullName user full name.
         */
        public User(Integer id, String fullName) {
            this.id = id;
            this.fullName = fullName;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            User person = (User)o;

            return Objects.equals(id, person.id) &&
                    Objects.equals(fullName, person.fullName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, fullName);
        }
    }
}
