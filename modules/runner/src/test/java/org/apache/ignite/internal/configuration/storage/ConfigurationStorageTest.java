/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.configuration.storage;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Base class for testing {@link ConfigurationStorage} implementations.
 */
public abstract class ConfigurationStorageTest {
    /**
     *
     */
    private ConfigurationStorage storage;

    /**
     * Returns the storage being tested.
     */
    public abstract ConfigurationStorage getStorage();

    /**
     *
     */
    @BeforeEach
    void setUp() {
        storage = getStorage();

        storage.registerConfigurationListener(data -> CompletableFuture.completedFuture(null));
    }

    /**
     * Tests the {@link ConfigurationStorage#readAllLatest} method.
     */
    @Test
    public void testReadAllLatest() {
        var data = Map.of("foo1", "bar1", "foo2", "bar2");

        assertThat(storage.write(data, 0), willBe(equalTo(true)));

        // test that reading without a prefix retrieves all data
        Map<String, ? extends Serializable> latestData = storage.readAllLatest("");

        assertThat(latestData, equalTo(data));

        // test that reading with a common prefix retrieves all data
        latestData = storage.readAllLatest("foo");

        assertThat(latestData, equalTo(data));

        // test that reading with a specific prefix retrieves corresponding data
        latestData = storage.readAllLatest("foo1");

        assertThat(latestData, equalTo(Map.of("foo1", "bar1")));

        // test that reading with a nonexistent prefix retrieves no data
        latestData = storage.readAllLatest("baz");

        assertThat(latestData, anEmptyMap());
    }
}
