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

package org.apache.ignite.internal.configuration.direct;

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.directProxy;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.getByInternalId;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.ConfigurationListenOnlyException;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.Name;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for "direct" configuration properties.
 */
public class DirectPropertiesTest {
    /**
     * Direct root configuration schema.
     */
    @ConfigurationRoot(rootName = "root")
    public static class DirectConfigurationSchema {
        @Name("childTestName")
        @ConfigValue
        public DirectNestedConfigurationSchema child;

        @NamedConfigValue
        public DirectNestedConfigurationSchema children;

        @Value(hasDefault = true)
        public String directStr = "foo";
    }

    /**
     * Direct nested configuration schema.
     */
    @Config
    public static class DirectNestedConfigurationSchema {
        @InjectedName
        public String name;

        @InternalId
        public UUID id;

        @Value(hasDefault = true)
        public String str = "bar";

        @NamedConfigValue
        public DirectNested2ConfigurationSchema children2;
    }

    /**
     * Direct nested 2 configuration schema.
     */
    @Config
    public static class DirectNested2ConfigurationSchema {
        @InternalId
        public UUID id;

        @Value(hasDefault = true)
        public String str = "bar";
    }

    private final ConfigurationRegistry registry = new ConfigurationRegistry(
            List.of(DirectConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(LOCAL),
            List.of(),
            List.of()
    );

    @BeforeEach
    void setUp() {
        registry.start();

        registry.initializeDefaults();
    }

    @AfterEach
    void tearDown() {
        registry.stop();
    }

    /**
     * Tests that configuration values and nested configurations work correctly.
     */
    @Test
    public void testDirectProperties() {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        // Check all possible orderings.
        assertThat(directProxy(cfg).value().directStr(), is("foo"));
        assertThat(directProxy(cfg).directStr().value(), is("foo"));
        assertThat(directProxy(cfg.directStr()).value(), is("foo"));

        // Check access to more deep subconfigurations.
        assertThat(directProxy(cfg).value().child().str(), is("bar"));
        assertThat(directProxy(cfg).child().value().str(), is("bar"));
        assertThat(directProxy(cfg).child().str().value(), is("bar"));

        assertThat(directProxy(cfg.child()).value().str(), is("bar"));
        assertThat(directProxy(cfg.child()).str().value(), is("bar"));

        assertThat(directProxy(cfg.child().str()).value(), is("bar"));

        // Check that id is null from all possible paths.
        assertThat(directProxy(cfg).value().child().id(), is(nullValue()));
        assertThat(directProxy(cfg).child().value().id(), is(nullValue()));
        assertThat(directProxy(cfg).child().id().value(), is(nullValue()));

        assertThat(directProxy(cfg.child()).value().id(), is(nullValue()));
        assertThat(directProxy(cfg.child()).id().value(), is(nullValue()));

        assertThat(directProxy(cfg.child().id()).value(), is(nullValue()));
    }

    /**
     * Same as {@link #testDirectProperties} but checks Named List properties.
     */
    @Test
    public void testNamedListDirectProperties() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.children()
                .change(change -> change.create("foo", value -> {}))
                .get(1, TimeUnit.SECONDS);

        UUID fooId = cfg.children().get("foo").id().value();

        assertThat(fooId, is(notNullValue()));

        // Check all possible ways to access "str" of element named "foo".
        assertThat(directProxy(cfg).value().children().get("foo").str(), is("bar"));
        assertThat(directProxy(cfg).children().value().get("foo").str(), is("bar"));
        assertThat(directProxy(cfg).children().get("foo").value().str(), is("bar"));
        assertThat(directProxy(cfg).children().get("foo").str().value(), is("bar"));

        assertThat(directProxy(cfg.children()).value().get("foo").str(), is("bar"));
        assertThat(directProxy(cfg.children()).get("foo").value().str(), is("bar"));
        assertThat(directProxy(cfg.children()).get("foo").str().value(), is("bar"));

        assertThat(directProxy(cfg.children().get("foo")).value().str(), is("bar"));
        assertThat(directProxy(cfg.children().get("foo")).str().value(), is("bar"));

        assertThat(directProxy(cfg.children().get("foo").str()).value(), is("bar"));

        // Check all possible ways to access "str" of element with given internal id.
        assertThat(getByInternalId(directProxy(cfg).value().children(), fooId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg).children().value(), fooId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg).children(), fooId).value().str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg).children(), fooId).str().value(), is("bar"));

        assertThat(getByInternalId(directProxy(cfg.children()).value(), fooId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children()), fooId).value().str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children()), fooId).str().value(), is("bar"));

        assertThat(directProxy(getByInternalId(cfg.children(), fooId)).value().str(), is("bar"));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId)).str().value(), is("bar"));

        assertThat(directProxy(getByInternalId(cfg.children(), fooId).str()).value(), is("bar"));
    }

    /**
     * Checks simple scenarios of getting internalId of named list element.
     */
    @Test
    public void testNamedListDirectInternalId() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.children()
                .change(change -> change.create("foo", value -> {
                }))
                .get(1, TimeUnit.SECONDS);

        UUID fooId = cfg.children().get("foo").id().value();

        assertThat(fooId, is(notNullValue()));

        // Check all possible ways to access "str" of element named "foo".
        assertThat(directProxy(cfg).value().children().get("foo").id(), is(equalTo(fooId)));
        assertThat(directProxy(cfg).children().value().get("foo").id(), is(equalTo(fooId)));
        assertThat(directProxy(cfg).children().get("foo").value().id(), is(equalTo(fooId)));
        assertThat(directProxy(cfg).children().get("foo").id().value(), is(equalTo(fooId)));

        assertThat(directProxy(cfg.children()).value().get("foo").id(), is(equalTo(fooId)));
        assertThat(directProxy(cfg.children()).get("foo").value().id(), is(equalTo(fooId)));
        assertThat(directProxy(cfg.children()).get("foo").id().value(), is(equalTo(fooId)));

        assertThat(directProxy(cfg.children().get("foo")).value().id(), is(equalTo(fooId)));
        assertThat(directProxy(cfg.children().get("foo")).id().value(), is(equalTo(fooId)));

        assertThat(directProxy(cfg.children().get("foo").id()).value(), is(equalTo(fooId)));

        // Check all possible ways to access "str" of element with given internal id.
        assertThat(getByInternalId(directProxy(cfg).value().children(), fooId).id(), is(equalTo(fooId)));
        assertThat(getByInternalId(directProxy(cfg).children().value(), fooId).id(), is(equalTo(fooId)));
        assertThat(getByInternalId(directProxy(cfg).children(), fooId).value().id(), is(equalTo(fooId)));
        assertThat(getByInternalId(directProxy(cfg).children(), fooId).id().value(), is(equalTo(fooId)));

        assertThat(getByInternalId(directProxy(cfg.children()).value(), fooId).id(), is(equalTo(fooId)));
        assertThat(getByInternalId(directProxy(cfg.children()), fooId).value().id(), is(equalTo(fooId)));
        assertThat(getByInternalId(directProxy(cfg.children()), fooId).id().value(), is(equalTo(fooId)));

        assertThat(directProxy(getByInternalId(cfg.children(), fooId)).value().id(), is(equalTo(fooId)));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId)).id().value(), is(equalTo(fooId)));

        assertThat(directProxy(getByInternalId(cfg.children(), fooId).id()).value(), is(equalTo(fooId)));
    }

    /**
     * Same as {@link #testDirectProperties} but checks Named List properties.
     */
    @Test
    public void testNamedListDirectNestedProperties() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.children()
                .change(list -> list.create("foo", e -> e.changeChildren2(list2 -> list2.create("boo", e2 -> {}))))
                .get(1, TimeUnit.SECONDS);

        UUID fooId = cfg.children().get("foo").id().value();
        UUID booId = cfg.children().get("foo").children2().get("boo").id().value();

        assertThat(booId, is(notNullValue()));

        // Check all possible ways to access "str", just to be sure. Some of these checks are clearly excessive, but they look organic.
        // Using names in both lists.
        assertThat(directProxy(cfg).value().children().get("foo").children2().get("boo").str(), is("bar"));
        assertThat(directProxy(cfg).children().value().get("foo").children2().get("boo").str(), is("bar"));
        assertThat(directProxy(cfg).children().get("foo").value().children2().get("boo").str(), is("bar"));
        assertThat(directProxy(cfg).children().get("foo").children2().value().get("boo").str(), is("bar"));
        assertThat(directProxy(cfg).children().get("foo").children2().get("boo").value().str(), is("bar"));
        assertThat(directProxy(cfg).children().get("foo").children2().get("boo").str().value(), is("bar"));

        assertThat(directProxy(cfg.children()).value().get("foo").children2().get("boo").str(), is("bar"));
        assertThat(directProxy(cfg.children()).get("foo").value().children2().get("boo").str(), is("bar"));
        assertThat(directProxy(cfg.children()).get("foo").children2().value().get("boo").str(), is("bar"));
        assertThat(directProxy(cfg.children()).get("foo").children2().get("boo").value().str(), is("bar"));
        assertThat(directProxy(cfg.children()).get("foo").children2().get("boo").str().value(), is("bar"));

        assertThat(directProxy(cfg.children().get("foo")).value().children2().get("boo").str(), is("bar"));
        assertThat(directProxy(cfg.children().get("foo")).children2().value().get("boo").str(), is("bar"));
        assertThat(directProxy(cfg.children().get("foo")).children2().get("boo").value().str(), is("bar"));
        assertThat(directProxy(cfg.children().get("foo")).children2().get("boo").str().value(), is("bar"));

        assertThat(directProxy(cfg.children().get("foo").children2()).value().get("boo").str(), is("bar"));
        assertThat(directProxy(cfg.children().get("foo").children2()).get("boo").value().str(), is("bar"));
        assertThat(directProxy(cfg.children().get("foo").children2()).get("boo").str().value(), is("bar"));

        assertThat(directProxy(cfg.children().get("foo").children2().get("boo")).value().str(), is("bar"));
        assertThat(directProxy(cfg.children().get("foo").children2().get("boo")).str().value(), is("bar"));

        assertThat(directProxy(cfg.children().get("foo").children2().get("boo").str()).value(), is("bar"));

        // Using internalId and name.
        assertThat(getByInternalId(directProxy(cfg).value().children(), fooId).children2().get("boo").str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg).children().value(), fooId).children2().get("boo").str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg).children(), fooId).value().children2().get("boo").str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg).children(), fooId).children2().value().get("boo").str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg).children(), fooId).children2().get("boo").value().str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg).children(), fooId).children2().get("boo").str().value(), is("bar"));

        assertThat(getByInternalId(directProxy(cfg.children()).value(), fooId).children2().get("boo").str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children()), fooId).value().children2().get("boo").str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children()), fooId).children2().value().get("boo").str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children()), fooId).children2().get("boo").value().str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children()), fooId).children2().get("boo").str().value(), is("bar"));

        assertThat(directProxy(getByInternalId(cfg.children(), fooId)).value().children2().get("boo").str(), is("bar"));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId)).children2().value().get("boo").str(), is("bar"));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId)).children2().get("boo").value().str(), is("bar"));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId)).children2().get("boo").str().value(), is("bar"));

        assertThat(directProxy(getByInternalId(cfg.children(), fooId).children2()).value().get("boo").str(), is("bar"));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId).children2()).get("boo").value().str(), is("bar"));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId).children2()).get("boo").str().value(), is("bar"));

        assertThat(directProxy(getByInternalId(cfg.children(), fooId).children2().get("boo")).value().str(), is("bar"));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId).children2().get("boo")).str().value(), is("bar"));

        assertThat(directProxy(getByInternalId(cfg.children(), fooId).children2().get("boo").str()).value(), is("bar"));

        // Using name and internalId.
        assertThat(getByInternalId(directProxy(cfg).value().children().get("foo").children2(), booId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg).children().value().get("foo").children2(), booId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg).children().get("foo").value().children2(), booId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg).children().get("foo").children2().value(), booId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg).children().get("foo").children2(), booId).value().str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg).children().get("foo").children2(), booId).str().value(), is("bar"));

        assertThat(getByInternalId(directProxy(cfg.children()).value().get("foo").children2(), booId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children()).get("foo").value().children2(), booId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children()).get("foo").children2().value(), booId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children()).get("foo").children2(), booId).value().str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children()).get("foo").children2(), booId).str().value(), is("bar"));

        assertThat(getByInternalId(directProxy(cfg.children().get("foo")).value().children2(), booId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children().get("foo")).children2().value(), booId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children().get("foo")).children2(), booId).value().str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children().get("foo")).children2(), booId).str().value(), is("bar"));

        assertThat(getByInternalId(directProxy(cfg.children().get("foo").children2()).value(), booId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children().get("foo").children2()), booId).value().str(), is("bar"));
        assertThat(getByInternalId(directProxy(cfg.children().get("foo").children2()), booId).str().value(), is("bar"));

        assertThat(directProxy(getByInternalId(cfg.children().get("foo").children2(), booId)).value().str(), is("bar"));
        assertThat(directProxy(getByInternalId(cfg.children().get("foo").children2(), booId)).str().value(), is("bar"));

        assertThat(directProxy(getByInternalId(cfg.children().get("foo").children2(), booId).str()).value(), is("bar"));

        // Using internalId and internalId.
        assertThat(getByInternalId(getByInternalId(directProxy(cfg).value().children(), fooId).children2(), booId).str(), is("bar"));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg).children().value(), fooId).children2(), booId).str(), is("bar"));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg).children(), fooId).value().children2(), booId).str(), is("bar"));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg).children(), fooId).children2().value(), booId).str(), is("bar"));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg).children(), fooId).children2(), booId).value().str(), is("bar"));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg).children(), fooId).children2(), booId).str().value(), is("bar"));

        assertThat(getByInternalId(getByInternalId(directProxy(cfg.children()).value(), fooId).children2(), booId).str(), is("bar"));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg.children()), fooId).value().children2(), booId).str(), is("bar"));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg.children()), fooId).children2().value(), booId).str(), is("bar"));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg.children()), fooId).children2(), booId).value().str(), is("bar"));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg.children()), fooId).children2(), booId).str().value(), is("bar"));

        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId)).value().children2(), booId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId)).children2().value(), booId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId)).children2(), booId).value().str(), is("bar"));
        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId)).children2(), booId).str().value(), is("bar"));

        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId).children2()).value(), booId).str(), is("bar"));
        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId).children2()), booId).value().str(), is("bar"));
        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId).children2()), booId).str().value(), is("bar"));

        assertThat(directProxy(getByInternalId(getByInternalId(cfg.children(), fooId).children2(), booId)).value().str(), is("bar"));
        assertThat(directProxy(getByInternalId(getByInternalId(cfg.children(), fooId).children2(), booId)).str().value(), is("bar"));

        assertThat(directProxy(getByInternalId(getByInternalId(cfg.children(), fooId).children2(), booId).str()).value(), is("bar"));
    }

    /**
     * Same as {@link #testDirectProperties} but checks Named List properties.
     */
    @Test
    public void testNamedListDirectNestedInternalId() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.children()
                .change(list -> list.create("foo", e -> e.changeChildren2(list2 -> list2.create("boo", e2 -> {}))))
                .get(1, TimeUnit.SECONDS);

        UUID fooId = cfg.children().get("foo").id().value();
        UUID booId = cfg.children().get("foo").children2().get("boo").id().value();

        assertThat(booId, is(notNullValue()));

        // Check all possible ways to access "str", just to be sure. Some of these checks are clearly excessive, but they look organic.
        // Using names in both lists.
        assertThat(directProxy(cfg).value().children().get("foo").children2().get("boo").id(), is(equalTo(booId)));
        assertThat(directProxy(cfg).children().value().get("foo").children2().get("boo").id(), is(equalTo(booId)));
        assertThat(directProxy(cfg).children().get("foo").value().children2().get("boo").id(), is(equalTo(booId)));
        assertThat(directProxy(cfg).children().get("foo").children2().value().get("boo").id(), is(equalTo(booId)));
        assertThat(directProxy(cfg).children().get("foo").children2().get("boo").value().id(), is(equalTo(booId)));
        assertThat(directProxy(cfg).children().get("foo").children2().get("boo").id().value(), is(equalTo(booId)));

        assertThat(directProxy(cfg.children()).value().get("foo").children2().get("boo").id(), is(equalTo(booId)));
        assertThat(directProxy(cfg.children()).get("foo").value().children2().get("boo").id(), is(equalTo(booId)));
        assertThat(directProxy(cfg.children()).get("foo").children2().value().get("boo").id(), is(equalTo(booId)));
        assertThat(directProxy(cfg.children()).get("foo").children2().get("boo").value().id(), is(equalTo(booId)));
        assertThat(directProxy(cfg.children()).get("foo").children2().get("boo").id().value(), is(equalTo(booId)));

        assertThat(directProxy(cfg.children().get("foo")).value().children2().get("boo").id(), is(equalTo(booId)));
        assertThat(directProxy(cfg.children().get("foo")).children2().value().get("boo").id(), is(equalTo(booId)));
        assertThat(directProxy(cfg.children().get("foo")).children2().get("boo").value().id(), is(equalTo(booId)));
        assertThat(directProxy(cfg.children().get("foo")).children2().get("boo").id().value(), is(equalTo(booId)));

        assertThat(directProxy(cfg.children().get("foo").children2()).value().get("boo").id(), is(equalTo(booId)));
        assertThat(directProxy(cfg.children().get("foo").children2()).get("boo").value().id(), is(equalTo(booId)));
        assertThat(directProxy(cfg.children().get("foo").children2()).get("boo").id().value(), is(equalTo(booId)));

        assertThat(directProxy(cfg.children().get("foo").children2().get("boo")).value().id(), is(equalTo(booId)));
        assertThat(directProxy(cfg.children().get("foo").children2().get("boo")).id().value(), is(equalTo(booId)));

        assertThat(directProxy(cfg.children().get("foo").children2().get("boo").id()).value(), is(equalTo(booId)));

        // Using internalId and name.
        assertThat(getByInternalId(directProxy(cfg).value().children(), fooId).children2().get("boo").id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg).children().value(), fooId).children2().get("boo").id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg).children(), fooId).value().children2().get("boo").id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg).children(), fooId).children2().value().get("boo").id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg).children(), fooId).children2().get("boo").value().id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg).children(), fooId).children2().get("boo").id().value(), is(equalTo(booId)));

        assertThat(getByInternalId(directProxy(cfg.children()).value(), fooId).children2().get("boo").id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg.children()), fooId).value().children2().get("boo").id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg.children()), fooId).children2().value().get("boo").id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg.children()), fooId).children2().get("boo").value().id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg.children()), fooId).children2().get("boo").id().value(), is(equalTo(booId)));

        assertThat(directProxy(getByInternalId(cfg.children(), fooId)).value().children2().get("boo").id(), is(equalTo(booId)));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId)).children2().value().get("boo").id(), is(equalTo(booId)));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId)).children2().get("boo").value().id(), is(equalTo(booId)));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId)).children2().get("boo").id().value(), is(equalTo(booId)));

        assertThat(directProxy(getByInternalId(cfg.children(), fooId).children2()).value().get("boo").id(), is(equalTo(booId)));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId).children2()).get("boo").value().id(), is(equalTo(booId)));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId).children2()).get("boo").id().value(), is(equalTo(booId)));

        assertThat(directProxy(getByInternalId(cfg.children(), fooId).children2().get("boo")).value().id(), is(equalTo(booId)));
        assertThat(directProxy(getByInternalId(cfg.children(), fooId).children2().get("boo")).id().value(), is(equalTo(booId)));

        assertThat(directProxy(getByInternalId(cfg.children(), fooId).children2().get("boo").id()).value(), is(equalTo(booId)));

        // Using name and internalId.
        assertThat(getByInternalId(directProxy(cfg).value().children().get("foo").children2(), booId).id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg).children().value().get("foo").children2(), booId).id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg).children().get("foo").value().children2(), booId).id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg).children().get("foo").children2().value(), booId).id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg).children().get("foo").children2(), booId).value().id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg).children().get("foo").children2(), booId).id().value(), is(equalTo(booId)));

        assertThat(getByInternalId(directProxy(cfg.children()).value().get("foo").children2(), booId).id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg.children()).get("foo").value().children2(), booId).id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg.children()).get("foo").children2().value(), booId).id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg.children()).get("foo").children2(), booId).value().id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg.children()).get("foo").children2(), booId).id().value(), is(equalTo(booId)));

        assertThat(getByInternalId(directProxy(cfg.children().get("foo")).value().children2(), booId).id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg.children().get("foo")).children2().value(), booId).id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg.children().get("foo")).children2(), booId).value().id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg.children().get("foo")).children2(), booId).id().value(), is(equalTo(booId)));

        assertThat(getByInternalId(directProxy(cfg.children().get("foo").children2()).value(), booId).id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg.children().get("foo").children2()), booId).value().id(), is(equalTo(booId)));
        assertThat(getByInternalId(directProxy(cfg.children().get("foo").children2()), booId).id().value(), is(equalTo(booId)));

        assertThat(directProxy(getByInternalId(cfg.children().get("foo").children2(), booId)).value().id(), is(equalTo(booId)));
        assertThat(directProxy(getByInternalId(cfg.children().get("foo").children2(), booId)).id().value(), is(equalTo(booId)));

        assertThat(directProxy(getByInternalId(cfg.children().get("foo").children2(), booId).id()).value(), is(equalTo(booId)));

        // Using internalId and internalId.
        assertThat(getByInternalId(getByInternalId(directProxy(cfg).value().children(), fooId).children2(), booId).id(), is(booId));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg).children().value(), fooId).children2(), booId).id(), is(booId));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg).children(), fooId).value().children2(), booId).id(), is(booId));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg).children(), fooId).children2().value(), booId).id(), is(booId));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg).children(), fooId).children2(), booId).value().id(), is(booId));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg).children(), fooId).children2(), booId).id().value(), is(booId));

        assertThat(getByInternalId(getByInternalId(directProxy(cfg.children()).value(), fooId).children2(), booId).id(), is(booId));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg.children()), fooId).value().children2(), booId).id(), is(booId));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg.children()), fooId).children2().value(), booId).id(), is(booId));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg.children()), fooId).children2(), booId).value().id(), is(booId));
        assertThat(getByInternalId(getByInternalId(directProxy(cfg.children()), fooId).children2(), booId).id().value(), is(booId));

        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId)).value().children2(), booId).id(), is(booId));
        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId)).children2().value(), booId).id(), is(booId));
        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId)).children2(), booId).value().id(), is(booId));
        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId)).children2(), booId).id().value(), is(booId));

        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId).children2()).value(), booId).id(), is(booId));
        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId).children2()), booId).value().id(), is(booId));
        assertThat(getByInternalId(directProxy(getByInternalId(cfg.children(), fooId).children2()), booId).id().value(), is(booId));

        assertThat(directProxy(getByInternalId(getByInternalId(cfg.children(), fooId).children2(), booId)).value().id(), is(booId));
        assertThat(directProxy(getByInternalId(getByInternalId(cfg.children(), fooId).children2(), booId)).id().value(), is(booId));

        assertThat(directProxy(getByInternalId(getByInternalId(cfg.children(), fooId).children2(), booId).id()).value(), is(booId));
    }

    @Test
    public void testNamedListNoSuchElement() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.children()
                .change(list -> list.create("foo", e -> e.changeChildren2(list2 -> list2.create("boo", e2 -> {}))))
                .get(1, TimeUnit.SECONDS);

        UUID fakeId = UUID.randomUUID();

        assertThrows(NoSuchElementException.class, () -> directProxy(cfg).children().get("a").value());
        assertThrows(NoSuchElementException.class, () -> getByInternalId(directProxy(cfg).children(), fakeId).value());

        DirectNestedConfiguration foo = cfg.children().get("foo");

        assertThrows(NoSuchElementException.class, () -> directProxy(foo).children2().get("b").value());
        assertThrows(NoSuchElementException.class, () -> getByInternalId(directProxy(foo).children2(), fakeId).value());
    }

    /**
     * Test for named list configuration.
     *
     * <p>Tests the following scenario:
     * <ol>
     *     <li>A Named List element is created. Both "direct" and regular properties must be the same.</li>
     *     <li>The element is removed. Both "direct" and regular properties must not return any values
     *     (i.e. throw an exception).</li>
     *     <li>A new Named List element is created under the same name. The regular property still must not return
     *     anything: the new element is represented by a different configuration node even if it has the same name.
     *     However, the "direct" property must return the new value, which is the intended behaviour.</li>
     * </ol>
     */
    @Test
    public void testNamedListDeleteCreate() {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        CompletableFuture<Void> changeFuture = cfg.children().change(change -> change.create("x", value -> {}));

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        DirectNestedConfiguration childCfg = cfg.children().get("x");

        assertThat(childCfg.str().value(), is("bar"));
        assertThat(directProxy(childCfg.str()).value(), is("bar"));

        changeFuture = cfg.children().change(change -> change.delete("x"));

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        assertThrows(NoSuchElementException.class, () -> childCfg.str().value());
        assertThrows(NoSuchElementException.class,
                () -> directProxy(childCfg.str()).value());

        changeFuture = cfg.children().change(change -> change.create("x", value -> {
        }));

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        assertThrows(NoSuchElementException.class, () -> childCfg.str().value());
        assertThat(directProxy(childCfg.str()).value(), is("bar"));
    }

    @Test
    void testDirectAccessForAny() {
        NamedConfigurationTree<DirectNestedConfiguration, DirectNestedView, DirectNestedChange> children =
                registry.getConfiguration(DirectConfiguration.KEY).children();

        assertThrows(ConfigurationListenOnlyException.class, () -> directProxy(children.any()));
        assertThrows(ConfigurationListenOnlyException.class, () -> directProxy(children.any().str()));
        assertThrows(ConfigurationListenOnlyException.class, () -> directProxy(children.any().children2()));
    }

    @Test
    void testInjectedNameNestedConfig() {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        // From root config.
        assertThat(directProxy(cfg).child().name().value(), is("childTestName"));
        assertThat(directProxy(cfg).value().child().name(), is("childTestName"));

        // From nested (child) config.
        assertThat(directProxy(cfg.child()).name().value(), is("childTestName"));
        assertThat(directProxy(cfg.child()).value().name(), is("childTestName"));

        // From config property.
        assertThat(directProxy(cfg.child().name()).value(), is("childTestName"));
    }

    @Test
    void testInjectedNameNamedConfig() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.change(c0 -> c0.changeChildren(c1 -> c1.create("0", c2 -> {}))).get(1, TimeUnit.SECONDS);

        // From root config.
        assertThat(directProxy(cfg).children().get("0").name().value(), is("0"));
        assertThat(directProxy(cfg).value().children().get("0").name(), is("0"));
        assertThat(directProxy(cfg).children().value().get("0").name(), is("0"));
        assertThat(directProxy(cfg).children().get("0").value().name(), is("0"));

        // From nested (children) config.
        assertThat(directProxy(cfg.children()).get("0").name().value(), is("0"));
        assertThat(directProxy(cfg.children()).value().get("0").name(), is("0"));
        assertThat(directProxy(cfg.children()).get("0").value().name(), is("0"));

        // From named element.
        assertThat(directProxy(cfg.children().get("0")).name().value(), is("0"));
        assertThat(directProxy(cfg.children().get("0")).value().name(), is("0"));

        // From named element config property.
        assertThat(directProxy(cfg.children().get("0").name()).value(), is("0"));
    }
}
