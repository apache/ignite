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

package org.apache.ignite.internal.configuration;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.ConfigurationListenOnlyException;
import org.apache.ignite.configuration.DirectConfigurationProperty;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.DirectAccess;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.directValue;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for "direct" configuration properties.
 *
 * @see DirectAccess
 */
public class DirectPropertiesTest {
    /** */
    @ConfigurationRoot(rootName = "root")
    @DirectAccess
    public static class DirectConfigurationSchema {
        /** */
        @ConfigValue
        public DirectNestedConfigurationSchema child;

        /** */
        @NamedConfigValue
        public DirectNestedConfigurationSchema childrenList;

        /** */
        @Value(hasDefault = true)
        @DirectAccess
        public String directStr = "foo";

        /** */
        @Value(hasDefault = true)
        public String nonDirectStr = "bar";
    }

    /** */
    @Config
    @DirectAccess
    public static class DirectNestedConfigurationSchema {
        /** */
        @Value(hasDefault = true)
        @DirectAccess
        public String str = "bar";

        /** */
        @Value(hasDefault = true)
        public String nonDirectStr = "bar";

        /** */
        @NamedConfigValue
        public DirectNested2ConfigurationSchema childrenList2;
    }

    /** */
    @Config
    @DirectAccess
    public static class DirectNested2ConfigurationSchema {
        /** */
        @Value(hasDefault = true)
        @DirectAccess
        public String str = "bar";

        /** */
        @Value(hasDefault = true)
        public String nonDirectStr = "bar";
    }

    /** */
    private final ConfigurationRegistry registry = new ConfigurationRegistry(
        List.of(DirectConfiguration.KEY),
        Map.of(),
        new TestConfigurationStorage(LOCAL),
        List.of(),
        List.of()
    );

    /** */
    @BeforeEach
    void setUp() {
        registry.start();

        registry.initializeDefaults();
    }

    /** */
    @AfterEach
    void tearDown() {
        registry.stop();
    }

    /**
     * Tests that configuration values and nested configurations are correctly compiled: properties marked with
     * {@link DirectAccess} should extend {@link DirectConfigurationProperty} and provide correct values.
     */
    @Test
    public void testDirectProperties() {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        assertThat(directValue(cfg).directStr(), is("foo"));
        assertThat(directValue(cfg.directStr()), is("foo"));

        assertThat(cfg.nonDirectStr(), not(instanceOf(DirectConfigurationProperty.class)));
        assertThat(directValue(cfg).nonDirectStr(), not(instanceOf(DirectConfigurationProperty.class)));

        assertThat(directValue(cfg.child()).str(), is("bar"));
        assertThat(directValue(cfg.child().str()), is("bar"));

        assertThat(cfg.child().nonDirectStr(), not(instanceOf(DirectConfigurationProperty.class)));
        assertThat(directValue(cfg.child()).nonDirectStr(), not(instanceOf(DirectConfigurationProperty.class)));
    }

    /**
     * Same as {@link #testDirectProperties} but checks Named List properties.
     */
    @Test
    public void testNamedListDirectProperties() throws Exception {
        DirectConfiguration cfg = registry.getConfiguration(DirectConfiguration.KEY);

        cfg.childrenList()
            .change(change -> change.create("foo", value -> {}))
            .get(1, TimeUnit.SECONDS);

        assertThat(directValue(cfg.childrenList()).get("foo").str(), is("bar"));
        assertThat(directValue(cfg.childrenList().get("foo")).str(), is("bar"));

        assertThat(cfg.childrenList().get("foo").nonDirectStr(), not(instanceOf(DirectConfigurationProperty.class)));
        assertThat(
            directValue(cfg.childrenList()).get("foo").nonDirectStr(),
            not(instanceOf(DirectConfigurationProperty.class))
        );
        assertThat(
            directValue(cfg.childrenList().get("foo")).nonDirectStr(),
            not(instanceOf(DirectConfigurationProperty.class))
        );
    }

    /**
     * Tests the following scenario:
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

        CompletableFuture<Void> changeFuture = cfg.childrenList().change(change -> change.create("x", value -> {}));

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        DirectNestedConfiguration childCfg = cfg.childrenList().get("x");

        assertThat(childCfg.str().value(), is("bar"));
        assertThat(directValue(childCfg.str()), is("bar"));

        changeFuture = cfg.childrenList().change(change -> change.delete("x"));

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        assertThrows(NoSuchElementException.class, () -> childCfg.str().value());
        assertThrows(NoSuchElementException.class, () -> directValue(childCfg.str()));

        changeFuture = cfg.childrenList().change(change -> change.create("x", value -> {}));

        assertThat(changeFuture, willBe(nullValue(Void.class)));

        assertThrows(NoSuchElementException.class, () -> childCfg.str().value());
        assertThat(directValue(childCfg.str()), is("bar"));
    }

    /** */
    @Test
    void testDirectAccessForAny() {
        NamedConfigurationTree<DirectNestedConfiguration, DirectNestedView, DirectNestedChange> childrenList =
            registry.getConfiguration(DirectConfiguration.KEY).childrenList();

        assertThrows(ConfigurationListenOnlyException.class, () -> directValue(childrenList.any()));
        assertThrows(ConfigurationListenOnlyException.class, () -> directValue(childrenList.any().str()));
        assertThrows(ConfigurationListenOnlyException.class, () -> directValue(childrenList.any().childrenList2()));
    }
}
