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

package org.apache.ignite.internal.suggestions;

import java.util.List;

import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link JvmConfigurationSuggestions}.
 */
public class JvmConfigurationSuggestionsTest {
    /***/
    @Test
    public void shouldSuggestJava11WhenNotRunOnJava11() {
        try (JdkVersionForger ignored = withJdkVersion("1.8")) {
            List<String> suggestions = JvmConfigurationSuggestions.getSuggestions();

            assertThat(suggestions, hasItem("Switch to the most recent 11 JVM version"));
        }
    }

    /***/
    @Test
    public void shouldNotSuggestJava11WhenRunOnJava11() {
        try (JdkVersionForger ignored = withJdkVersion("11")) {
            List<String> suggestions = JvmConfigurationSuggestions.getSuggestions();

            assertThat(suggestions, not(hasItem(matchesPattern("Switch to the most recent .+"))));
        }
    }

    /***/
    @NotNull
    private JdkVersionForger withJdkVersion(String jdkVersion) {
        return new JdkVersionForger(jdkVersion);
    }

    /**
     * Forges JDK version stored as {@code IgniteUtils#jdkVer}. Normally, we would just use
     * {@code @WithSystemProperty} and be happy, but {@link IgniteUtils} caches values read from system properties
     * on startup, so we have to resort to this dirty trick.
     */
    private static class JdkVersionForger implements AutoCloseable {
        /** The JDK version we saw in the field when constructing, used to restore the original value. */
        private final String oldVersion = IgniteUtils.jdkVersion();

        /***/
        private JdkVersionForger(String newVersion) {
            setJdkVersion(newVersion);
        }

        /***/
        private void setJdkVersion(String newVersion) {
            GridTestUtils.setFieldValue(null, IgniteUtils.class, "jdkVer", newVersion);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            setJdkVersion(oldVersion);
        }
    }
}
