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

package org.apache.ignite.internal.configuration.notifications;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.checkContainsListeners;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.configListener;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.configNamedListenerOnCreate;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.configNamedListenerOnDelete;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.configNamedListenerOnRename;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.configNamedListenerOnUpdate;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.doNothingConsumer;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotifier.notifyListeners;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class for configuration listener.
 */
public class ConfigurationListenerTest {
    /**
     * Parent root configuration schema.
     */
    @ConfigurationRoot(rootName = "parent", type = LOCAL)
    public static class ParentConfigurationSchema {
        @ConfigValue
        public ChildConfigurationSchema child;

        @NamedConfigValue
        public ChildConfigurationSchema children;

        @ConfigValue
        public PolyConfigurationSchema polyChild;

        @NamedConfigValue
        public PolyConfigurationSchema polyChildren;
    }

    /**
     * Child configuration schema.
     */
    @Config
    public static class ChildConfigurationSchema {
        @Value(hasDefault = true)
        public String str = "default";
    }

    /**
     * Internal extension of {@link ChildConfigurationSchema}.
     */
    @InternalConfiguration
    public static class InternalChildConfigurationSchema extends ChildConfigurationSchema {
        @Value(hasDefault = true)
        public int intVal = 0;
    }

    /**
     * Base class for polymorhphic configs.
     */
    @PolymorphicConfig
    public static class PolyConfigurationSchema {
        public static final String STRING = "string";
        public static final String LONG = "long";

        @PolymorphicId(hasDefault = true)
        public String type = STRING;

        @Value(hasDefault = true)
        public int commonIntVal = 11;
    }

    /**
     * String-based variant of a polymorphic config.
     */
    @PolymorphicConfigInstance(PolyConfigurationSchema.STRING)
    public static class StringPolyConfigurationSchema extends PolyConfigurationSchema {
        @Value(hasDefault = true)
        public String specificVal = "original";
    }

    /**
     * Long-based variant of a polymorphic config.
     */
    @PolymorphicConfigInstance(PolyConfigurationSchema.LONG)
    public static class LongPolyConfigurationSchema extends PolyConfigurationSchema {
        @Value(hasDefault = true)
        public long specificVal = 12;
    }

    private ConfigurationRegistry registry;

    private ParentConfiguration config;

    /**
     * Before each.
     */
    @BeforeEach
    public void before() {
        var testConfigurationStorage = new TestConfigurationStorage(LOCAL);

        registry = new ConfigurationRegistry(
                List.of(ParentConfiguration.KEY),
                Map.of(),
                testConfigurationStorage,
                List.of(InternalChildConfigurationSchema.class),
                List.of(StringPolyConfigurationSchema.class, LongPolyConfigurationSchema.class)
        );

        registry.start();

        registry.initializeDefaults();

        config = registry.getConfiguration(ParentConfiguration.KEY);
    }

    @AfterEach
    public void after() {
        registry.stop();
    }

    @Test
    public void childNode() throws Exception {
        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            assertEquals(ctx.oldValue().child().str(), "default");
            assertEquals(ctx.newValue().child().str(), "foo");

            log.add("parent");

            return completedFuture(null);
        });

        config.child().listen(ctx -> {
            assertEquals(ctx.oldValue().str(), "default");
            assertEquals(ctx.newValue().str(), "foo");

            log.add("child");

            return completedFuture(null);
        });

        config.child().str().listen(ctx -> {
            assertEquals(ctx.oldValue(), "default");
            assertEquals(ctx.newValue(), "foo");

            log.add("str");

            return completedFuture(null);
        });

        config.children().listen(ctx -> {
            log.add("elements");

            return completedFuture(null);
        });

        config.change(parent -> parent.changeChild(child -> child.changeStr("foo"))).get(1, SECONDS);

        assertEquals(List.of("parent", "child", "str"), log);
    }

    /**
     * Tests notifications validity when a new named list element is created.
     */
    @Test
    public void namedListNodeOnCreate() throws Exception {
        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        config.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        config.children().listen(ctx -> {
            assertEquals(0, ctx.oldValue().size());

            ChildView newValue = ctx.newValue().get("name");

            assertNotNull(newValue);
            assertEquals("default", newValue.str());

            log.add("elements");

            return completedFuture(null);
        });

        config.children().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                assertNull(ctx.oldValue());

                ChildView newValue = ctx.newValue();

                assertNotNull(newValue);
                assertEquals("default", newValue.str());

                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onRename(
                    String oldName,
                    String newName,
                    ConfigurationNotificationEvent<ChildView> ctx
            ) {
                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return completedFuture(null);
            }
        });

        config.change(parent ->
                parent.changeChildren(elements -> elements.create("name", element -> {
                }))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "create"), log);
    }

    /**
     * Tests notifications validity when a named list element is edited.
     */
    @Test
    public void namedListNodeOnUpdate() throws Exception {
        config.change(parent ->
                parent.changeChildren(elements -> elements.create("name", element -> {
                }))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        config.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        config.children().listen(ctx -> {

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            ChildView newValue = ctx.newValue().get("name");

            assertNotNull(newValue);
            assertEquals("foo", newValue.str());

            log.add("elements");

            return completedFuture(null);
        });

        config.children().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                ChildView newValue = ctx.newValue();

                assertNotNull(newValue);
                assertEquals("foo", newValue.str());

                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onRename(
                    String oldName,
                    String newName,
                    ConfigurationNotificationEvent<ChildView> ctx
            ) {
                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return completedFuture(null);
            }
        });

        config.change(parent ->
                parent.changeChildren(elements -> elements.createOrUpdate("name", element -> element.changeStr("foo")))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "update"), log);
    }

    /**
     * Tests notifications validity when a named list element is renamed.
     */
    @Test
    public void namedListNodeOnRename() throws Exception {
        config.change(parent ->
                parent.changeChildren(elements -> elements.create("name", element -> {
                }))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        config.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        config.children().listen(ctx -> {
            assertEquals(1, ctx.oldValue().size());

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            assertEquals(1, ctx.newValue().size());

            ChildView newValue = ctx.newValue().get("newName");

            assertSame(oldValue, newValue);

            log.add("elements");

            return completedFuture(null);
        });

        config.children().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onRename(
                    String oldName,
                    String newName,
                    ConfigurationNotificationEvent<ChildView> ctx
            ) {
                assertEquals("name", oldName);
                assertEquals("newName", newName);

                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                ChildView newValue = ctx.newValue();

                assertSame(oldValue, newValue);

                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return completedFuture(null);
            }
        });

        config.change(parent ->
                parent.changeChildren(elements -> elements.rename("name", "newName"))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "rename"), log);
    }

    /**
     * Tests notifications validity when a named list element is renamed and updated at the same time.
     */
    @Test
    public void namedListNodeOnRenameAndUpdate() throws Exception {
        config.change(parent ->
                parent.changeChildren(elements -> elements.create("name", element -> {
                }))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        config.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        config.children().listen(ctx -> {
            assertEquals(1, ctx.oldValue().size());

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            assertEquals(1, ctx.newValue().size());

            ChildView newValue = ctx.newValue().get("newName");

            assertNotNull(newValue, ctx.newValue().namedListKeys().toString());
            assertEquals("foo", newValue.str());

            log.add("elements");

            return completedFuture(null);
        });

        config.children().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onRename(
                    String oldName,
                    String newName,
                    ConfigurationNotificationEvent<ChildView> ctx
            ) {
                assertEquals("name", oldName);
                assertEquals("newName", newName);

                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                ChildView newValue = ctx.newValue();

                assertNotNull(newValue);
                assertEquals("foo", newValue.str());

                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return completedFuture(null);
            }
        });

        config.change(parent ->
                parent.changeChildren(elements -> elements
                        .rename("name", "newName")
                        .createOrUpdate("newName", element -> element.changeStr("foo"))
                )
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "rename"), log);
    }

    /**
     * Tests notifications validity when a named list element is deleted.
     */
    @Test
    public void namedListNodeOnDelete() throws Exception {
        config.change(parent ->
                parent.changeChildren(elements -> elements.create("name", element -> {
                }))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        config.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        config.children().listen(ctx -> {
            assertEquals(0, ctx.newValue().size());

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            log.add("elements");

            return completedFuture(null);
        });

        config.children().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onRename(
                    String oldName,
                    String newName,
                    ConfigurationNotificationEvent<ChildView> ctx
            ) {
                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                assertNull(ctx.newValue());

                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                log.add("delete");

                return completedFuture(null);
            }
        });

        config.children().get("name").listen(ctx -> {
            return completedFuture(null);
        });

        config.change(parent ->
                parent.changeChildren(elements -> elements.delete("name"))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "delete"), log);
    }

    /**
     * Tests that concurrent configuration access does not affect configuration listeners.
     */
    @Test
    public void dataRace() throws Exception {
        config.change(parent -> parent.changeChildren(elements ->
                elements.create("name", e -> {
                }))
        ).get(1, SECONDS);

        CountDownLatch wait = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            try {
                wait.await(1, SECONDS);
            } catch (InterruptedException e) {
                fail(e.getMessage());
            }

            release.countDown();

            return completedFuture(null);
        });

        config.children().get("name").listen(ctx -> {
            assertNull(ctx.newValue());

            log.add("deleted");

            return completedFuture(null);
        });

        final Future<Void> fut = config.change(parent -> parent.changeChildren(elements ->
                elements.delete("name"))
        );

        wait.countDown();

        config.children();

        release.await(1, SECONDS);

        fut.get(1, SECONDS);

        assertEquals(List.of("deleted"), log);
    }

    @Test
    void testStopListen() throws Exception {
        List<String> events = new ArrayList<>();

        ConfigurationListener<ParentView> listener0 = configListener(ctx -> events.add("0"));
        ConfigurationListener<ParentView> listener1 = configListener(ctx -> events.add("1"));

        ConfigurationNamedListListener<ChildView> listener2 = configNamedListenerOnUpdate(ctx -> events.add("2"));
        final ConfigurationNamedListListener<ChildView> listener3 = configNamedListenerOnUpdate(ctx -> events.add("3"));

        config.listen(listener0);
        config.listen(listener1);

        config.children().listenElements(listener2);
        config.children().listenElements(listener3);

        config.children().change(c -> c.create("0", doNothingConsumer())).get(1, SECONDS);

        checkContainsListeners(
                () -> config.children().get("0").str().update(UUID.randomUUID().toString()),
                events,
                List.of("0", "1", "2", "3"),
                List.of()
        );

        config.stopListen(listener0);
        config.children().stopListenElements(listener2);

        checkContainsListeners(
                () -> config.children().get("0").str().update(UUID.randomUUID().toString()),
                events,
                List.of("1", "3"),
                List.of("0", "2")
        );

        config.stopListen(listener1);
        config.children().stopListenElements(listener3);

        checkContainsListeners(
                () -> config.children().get("0").str().update(UUID.randomUUID().toString()),
                events,
                List.of(),
                List.of("0", "1", "2", "3")
        );
    }

    @Test
    void testGetConfigFromNotificationEvent() throws Exception {
        String newVal = UUID.randomUUID().toString();

        config.listen(configListener(ctx -> {
            ParentConfiguration parent = ctx.config(ParentConfiguration.class);

            assertNotNull(parent);
            assertNull(ctx.name(ParentConfiguration.class));

            assertEquals(newVal, parent.child().str().value());
        }));

        config.child().listen(configListener(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));

            ChildConfiguration child = ctx.config(ChildConfiguration.class);

            assertNotNull(child);
            assertNull(ctx.name(ChildConfiguration.class));

            assertEquals(newVal, child.str().value());
        }));

        config.child().str().listen(configListener(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));

            ChildConfiguration child = ctx.config(ChildConfiguration.class);

            assertNotNull(child);
            assertNull(ctx.name(ChildConfiguration.class));

            assertEquals(newVal, child.str().value());
        }));

        config.change(c0 -> c0.changeChild(c1 -> c1.changeStr(newVal))).get(1, SECONDS);
    }

    @Test
    void testGetConfigFromNotificationEventOnCreate() throws Exception {
        String newVal = UUID.randomUUID().toString();
        String key = UUID.randomUUID().toString();

        config.children().listen(configListener(ctx -> {
            ParentConfiguration parent = ctx.config(ParentConfiguration.class);

            assertNotNull(parent);
            assertNull(ctx.name(ParentConfiguration.class));

            assertNull(ctx.config(ChildConfiguration.class));
            assertNull(ctx.name(ChildConfiguration.class));

            assertEquals(newVal, parent.children().get(key).str().value());
        }));

        config.children().listenElements(configNamedListenerOnCreate(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));
            assertNull(ctx.name(ParentConfiguration.class));

            ChildConfiguration child = ctx.config(ChildConfiguration.class);

            assertNotNull(child);
            assertEquals(key, ctx.name(ChildConfiguration.class));

            assertEquals(newVal, child.str().value());
        }));

        config.children().change(c -> c.create(key, c1 -> c1.changeStr(newVal))).get(1, SECONDS);
    }

    @Test
    void testGetConfigFromNotificationEventOnRename() throws Exception {
        String val = "default";
        String oldKey = UUID.randomUUID().toString();
        String newKey = UUID.randomUUID().toString();

        config.children().change(c -> c.create(oldKey, doNothingConsumer())).get(1, SECONDS);

        config.children().listen(configListener(ctx -> {
            ParentConfiguration parent = ctx.config(ParentConfiguration.class);

            assertNotNull(parent);
            assertNull(ctx.name(ParentConfiguration.class));

            assertNull(ctx.config(ChildConfiguration.class));
            assertNull(ctx.name(ChildConfiguration.class));

            assertNull(parent.children().get(oldKey));
            assertEquals(val, parent.children().get(newKey).str().value());
        }));

        config.children().listenElements(configNamedListenerOnRename(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));
            assertNull(ctx.name(ParentConfiguration.class));

            ChildConfiguration child = ctx.config(ChildConfiguration.class);

            assertNotNull(child);
            assertEquals(newKey, ctx.name(ChildConfiguration.class));

            assertEquals(val, child.str().value());
        }));

        config.children().change(c -> c.rename(oldKey, newKey));
    }

    @Test
    void testGetConfigFromNotificationEventOnDelete() throws Exception {
        String key = UUID.randomUUID().toString();

        config.children().change(c -> c.create(key, doNothingConsumer())).get(1, SECONDS);

        config.children().listen(configListener(ctx -> {
            ParentConfiguration parent = ctx.config(ParentConfiguration.class);

            assertNotNull(parent);
            assertNull(ctx.name(ParentConfiguration.class));

            assertNull(ctx.config(ChildConfiguration.class));
            assertNull(ctx.name(ChildConfiguration.class));

            assertNull(parent.children().get(key));
        }));

        config.children().listenElements(configNamedListenerOnDelete(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));
            assertNull(ctx.name(ParentConfiguration.class));

            assertNull(ctx.config(ChildConfiguration.class));
            assertEquals(key, ctx.name(ChildConfiguration.class));
        }));

        config.children().get(key).listen(configListener(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));
            assertNull(ctx.name(ParentConfiguration.class));

            assertNull(ctx.config(ChildConfiguration.class));
            assertEquals(key, ctx.name(ChildConfiguration.class));
        }));

        config.children().change(c -> c.delete(key)).get(1, SECONDS);
    }

    @Test
    void testGetConfigFromNotificationEventOnUpdate() throws Exception {
        String newVal = UUID.randomUUID().toString();
        String key = UUID.randomUUID().toString();

        config.children().change(c -> c.create(key, doNothingConsumer())).get(1, SECONDS);

        config.children().listen(configListener(ctx -> {
            ParentConfiguration parent = ctx.config(ParentConfiguration.class);

            assertNotNull(parent);
            assertNull(ctx.name(ParentConfiguration.class));

            assertNull(ctx.config(ChildConfiguration.class));
            assertNull(ctx.name(ChildConfiguration.class));

            assertEquals(newVal, parent.children().get(key).str().value());
        }));

        config.children().listenElements(configNamedListenerOnUpdate(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));
            assertNull(ctx.name(ParentConfiguration.class));

            ChildConfiguration child = ctx.config(ChildConfiguration.class);

            assertNotNull(child);
            assertEquals(key, ctx.name(ChildConfiguration.class));

            assertEquals(newVal, child.str().value());
        }));

        config.children().get(key).listen(configListener(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));
            assertNull(ctx.name(ParentConfiguration.class));

            ChildConfiguration child = ctx.config(ChildConfiguration.class);

            assertNotNull(child);
            assertEquals(key, ctx.name(ChildConfiguration.class));

            assertEquals(newVal, child.str().value());
        }));

        config.children().get(key).str().update(newVal).get(1, SECONDS);
    }

    @Test
    void polymorphicParentFieldChangeNotificationHappens() throws Exception {
        AtomicInteger intHolder = new AtomicInteger();

        config.polyChild().commonIntVal().listen(event -> {
            intHolder.set(event.newValue());
            return CompletableFuture.completedFuture(null);
        });

        config.polyChild().commonIntVal().update(42).get(1, SECONDS);

        assertThat(intHolder.get(), is(42));
    }

    @Test
    void testNotificationEventConfigForNestedConfiguration() throws Exception {
        config.child().listen(ctx -> {
            assertInstanceOf(ChildConfiguration.class, ctx.config(ChildConfiguration.class));
            assertInstanceOf(InternalChildConfiguration.class, ctx.config(InternalChildConfiguration.class));

            assertNull(ctx.name(ChildConfiguration.class));
            assertNull(ctx.name(InternalChildConfiguration.class));

            return CompletableFuture.completedFuture(null);
        });

        config.child().str().update(UUID.randomUUID().toString()).get(1, SECONDS);
    }

    @Test
    void testNotificationEventConfigForNamedConfiguration() throws Exception {
        config.children().listenElements(new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override
            public @NotNull CompletableFuture<?> onCreate(@NotNull ConfigurationNotificationEvent<ChildView> ctx) {
                assertInstanceOf(ChildConfiguration.class, ctx.config(ChildConfiguration.class));
                assertInstanceOf(InternalChildConfiguration.class, ctx.config(InternalChildConfiguration.class));

                assertEquals("0", ctx.name(ChildConfiguration.class));
                assertEquals("0", ctx.name(InternalChildConfiguration.class));

                return CompletableFuture.completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public @NotNull CompletableFuture<?> onRename(
                    @NotNull String oldName,
                    @NotNull String newName,
                    @NotNull ConfigurationNotificationEvent<ChildView> ctx
            ) {
                assertInstanceOf(ChildConfiguration.class, ctx.config(ChildConfiguration.class));
                assertInstanceOf(InternalChildConfiguration.class, ctx.config(InternalChildConfiguration.class));

                assertEquals("1", ctx.name(ChildConfiguration.class));
                assertEquals("1", ctx.name(InternalChildConfiguration.class));

                return CompletableFuture.completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public @NotNull CompletableFuture<?> onDelete(@NotNull ConfigurationNotificationEvent<ChildView> ctx) {
                assertNull(ctx.config(ChildConfiguration.class));
                assertNull(ctx.config(InternalChildConfiguration.class));

                assertEquals("1", ctx.name(ChildConfiguration.class));
                assertEquals("1", ctx.name(InternalChildConfiguration.class));

                return CompletableFuture.completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public @NotNull CompletableFuture<?> onUpdate(@NotNull ConfigurationNotificationEvent<ChildView> ctx) {
                assertInstanceOf(ChildConfiguration.class, ctx.config(ChildConfiguration.class));
                assertInstanceOf(InternalChildConfiguration.class, ctx.config(InternalChildConfiguration.class));

                assertEquals("1", ctx.name(ChildConfiguration.class));
                assertEquals("1", ctx.name(InternalChildConfiguration.class));

                return CompletableFuture.completedFuture(null);
            }
        });

        config.children().change(c -> c.create("0", c1 -> {})).get(1, SECONDS);
        config.children().change(c -> c.rename("0", "1")).get(1, SECONDS);
        config.children().change(c -> c.update("1", c1 -> c1.changeStr(UUID.randomUUID().toString()))).get(1, SECONDS);
        config.children().change(c -> c.delete("1")).get(1, SECONDS);
    }

    @Test
    void testNotificationEventConfigForNestedPolymorphicConfiguration() throws Exception {
        config.polyChild().listen(ctx -> {
            assertInstanceOf(PolyConfiguration.class, ctx.config(PolyConfiguration.class));
            assertInstanceOf(StringPolyConfiguration.class, ctx.config(StringPolyConfiguration.class));

            assertNull(ctx.config(LongPolyConfiguration.class));

            assertNull(ctx.name(PolyConfiguration.class));
            assertNull(ctx.name(StringPolyConfiguration.class));
            assertNull(ctx.name(LongPolyConfiguration.class));

            return CompletableFuture.completedFuture(null);
        });

        config.polyChild().commonIntVal().update(22).get(1, SECONDS);
    }

    @Test
    void testNotificationEventConfigForNamedPolymorphicConfiguration() throws Exception {
        config.polyChildren().listenElements(new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override
            public @NotNull CompletableFuture<?> onCreate(@NotNull ConfigurationNotificationEvent<PolyView> ctx) {
                assertInstanceOf(PolyConfiguration.class, ctx.config(PolyConfiguration.class));
                assertInstanceOf(StringPolyConfiguration.class, ctx.config(StringPolyConfiguration.class));

                assertNull(ctx.config(LongPolyConfiguration.class));

                assertEquals("0", ctx.name(PolyConfiguration.class));
                assertEquals("0", ctx.name(StringPolyConfiguration.class));

                assertNull(ctx.name(LongPolyConfiguration.class));

                return CompletableFuture.completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public @NotNull CompletableFuture<?> onRename(
                    @NotNull String oldName,
                    @NotNull String newName,
                    @NotNull ConfigurationNotificationEvent<PolyView> ctx
            ) {
                assertInstanceOf(PolyConfiguration.class, ctx.config(PolyConfiguration.class));
                assertInstanceOf(StringPolyConfiguration.class, ctx.config(StringPolyConfiguration.class));

                assertNull(ctx.config(LongPolyConfiguration.class));

                assertEquals("1", ctx.name(PolyConfiguration.class));
                assertEquals("1", ctx.name(StringPolyConfiguration.class));

                assertNull(ctx.name(LongPolyConfiguration.class));

                return CompletableFuture.completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public @NotNull CompletableFuture<?> onDelete(@NotNull ConfigurationNotificationEvent<PolyView> ctx) {
                assertNull(ctx.config(PolyConfiguration.class));
                assertNull(ctx.config(StringPolyConfiguration.class));
                assertNull(ctx.config(LongPolyConfiguration.class));

                assertEquals("1", ctx.name(PolyConfiguration.class));
                assertEquals("1", ctx.name(StringPolyConfiguration.class));

                assertNull(ctx.name(LongPolyConfiguration.class));

                return CompletableFuture.completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override
            public @NotNull CompletableFuture<?> onUpdate(@NotNull ConfigurationNotificationEvent<PolyView> ctx) {
                assertInstanceOf(PolyConfiguration.class, ctx.config(PolyConfiguration.class));
                assertInstanceOf(StringPolyConfiguration.class, ctx.config(StringPolyConfiguration.class));

                assertNull(ctx.config(LongPolyConfiguration.class));

                assertEquals("1", ctx.name(PolyConfiguration.class));
                assertEquals("1", ctx.name(StringPolyConfiguration.class));

                assertNull(ctx.name(LongPolyConfiguration.class));

                return CompletableFuture.completedFuture(null);
            }
        });

        config.polyChildren().change(c -> c.create("0", c1 -> {})).get(1, SECONDS);
        config.polyChildren().change(c -> c.rename("0", "1")).get(1, SECONDS);
        config.polyChildren().change(c -> c.update("1", c1 -> c1.changeCommonIntVal(22))).get(1, SECONDS);
        config.polyChildren().change(c -> c.delete("1")).get(1, SECONDS);
    }

    @Test
    void testNotificationListenerForNestedPolymorphicConfig() throws Exception {
        AtomicBoolean invokeListener = new AtomicBoolean();

        config.polyChild().listen(configListener(ctx -> {
            invokeListener.set(true);

            assertInstanceOf(PolyView.class, ctx.newValue());
            assertInstanceOf(LongPolyView.class, ctx.newValue());

            assertInstanceOf(PolyView.class, ctx.oldValue());
            assertInstanceOf(StringPolyView.class, ctx.oldValue());

            assertInstanceOf(PolyConfiguration.class, ctx.config(PolyConfiguration.class));
            assertInstanceOf(LongPolyConfiguration.class, ctx.config(LongPolyConfiguration.class));

            assertNull(ctx.config(StringPolyConfiguration.class));

            assertNull(ctx.name(PolyConfiguration.class));
            assertNull(ctx.name(LongPolyConfiguration.class));
            assertNull(ctx.name(StringPolyConfiguration.class));
        }));

        config.polyChild()
                .change(c -> c.convert(LongPolyChange.class).changeSpecificVal(0).changeCommonIntVal(0))
                .get(1, SECONDS);

        assertTrue(invokeListener.get());
    }

    @Test
    void testNotificationListenerOnCreateNamedPolymorphicConfig() throws Exception {
        AtomicBoolean invokeListener = new AtomicBoolean();

        config.polyChildren().listenElements(configNamedListenerOnCreate(ctx -> {
            invokeListener.set(true);

            assertInstanceOf(PolyView.class, ctx.newValue());
            assertInstanceOf(StringPolyView.class, ctx.newValue());

            assertNull(ctx.oldValue());

            assertInstanceOf(PolyConfiguration.class, ctx.config(PolyConfiguration.class));
            assertInstanceOf(StringPolyConfiguration.class, ctx.config(StringPolyConfiguration.class));

            assertNull(ctx.config(LongPolyConfiguration.class));

            assertEquals("0", ctx.name(PolyConfiguration.class));
            assertEquals("0", ctx.name(StringPolyConfiguration.class));

            assertNull(ctx.name(LongPolyConfiguration.class));
        }));

        config.polyChildren()
                .change(c -> c.create("0", c1 -> c1.convert(StringPolyChange.class).changeSpecificVal("").changeCommonIntVal(0)))
                .get(1, SECONDS);

        assertTrue(invokeListener.get());
    }

    @Test
    void testNotificationListenerOnUpdateNamedPolymorphicConfig() throws Exception {
        config.polyChildren()
                .change(c -> c.create("0", c1 -> c1.convert(StringPolyChange.class).changeSpecificVal("").changeCommonIntVal(0)))
                .get(1, SECONDS);

        AtomicBoolean invokeListener = new AtomicBoolean();

        config.polyChildren().listenElements(configNamedListenerOnUpdate(ctx -> {
            invokeListener.set(true);

            assertInstanceOf(PolyView.class, ctx.newValue());
            assertInstanceOf(LongPolyView.class, ctx.newValue());

            assertInstanceOf(PolyView.class, ctx.oldValue());
            assertInstanceOf(StringPolyView.class, ctx.oldValue());

            assertInstanceOf(PolyConfiguration.class, ctx.config(PolyConfiguration.class));
            assertInstanceOf(LongPolyConfiguration.class, ctx.config(LongPolyConfiguration.class));

            assertNull(ctx.config(StringPolyConfiguration.class));

            assertEquals("0", ctx.name(PolyConfiguration.class));
            assertEquals("0", ctx.name(LongPolyConfiguration.class));

            assertNull(ctx.name(StringPolyConfiguration.class));
        }));

        config.polyChildren()
                .change(c -> c.update("0", c1 -> c1.convert(LongPolyChange.class).changeSpecificVal(0).changeCommonIntVal(0)))
                .get(1, SECONDS);

        assertTrue(invokeListener.get());
    }

    @Test
    void testNotificationListenerOnRenameNamedPolymorphicConfig() throws Exception {
        config.polyChildren()
                .change(c -> c.create("0", c1 -> c1.convert(StringPolyChange.class).changeSpecificVal("").changeCommonIntVal(0)))
                .get(1, SECONDS);

        AtomicBoolean invokeListener = new AtomicBoolean();

        config.polyChildren().listenElements(configNamedListenerOnRename(ctx -> {
            invokeListener.set(true);

            assertInstanceOf(PolyView.class, ctx.newValue());
            assertInstanceOf(StringPolyView.class, ctx.newValue());

            assertInstanceOf(PolyView.class, ctx.oldValue());
            assertInstanceOf(StringPolyView.class, ctx.oldValue());

            assertInstanceOf(PolyConfiguration.class, ctx.config(PolyConfiguration.class));
            assertInstanceOf(StringPolyConfiguration.class, ctx.config(StringPolyConfiguration.class));

            assertNull(ctx.config(LongPolyConfiguration.class));

            assertEquals("1", ctx.name(PolyConfiguration.class));
            assertEquals("1", ctx.name(StringPolyConfiguration.class));

            assertNull(ctx.name(LongPolyConfiguration.class));
        }));

        config.polyChildren()
                .change(c -> c.rename("0", "1"))
                .get(1, SECONDS);

        assertTrue(invokeListener.get());
    }

    @Test
    void testNotificationListenerOnDeleteNamedPolymorphicConfig() throws Exception {
        config.polyChildren()
                .change(c -> c.create("0", c1 -> c1.convert(StringPolyChange.class).changeSpecificVal("").changeCommonIntVal(0)))
                .get(1, SECONDS);

        AtomicBoolean invokeListener = new AtomicBoolean();

        config.polyChildren().listenElements(configNamedListenerOnDelete(ctx -> {
            invokeListener.set(true);

            assertNull(ctx.newValue());

            assertInstanceOf(PolyView.class, ctx.oldValue());
            assertInstanceOf(StringPolyView.class, ctx.oldValue());

            assertNull(ctx.config(PolyConfiguration.class));
            assertNull(ctx.config(StringPolyConfiguration.class));
            assertNull(ctx.config(LongPolyConfiguration.class));

            assertEquals("0", ctx.name(PolyConfiguration.class));
            assertEquals("0", ctx.name(StringPolyConfiguration.class));

            assertNull(ctx.name(LongPolyConfiguration.class));
        }));

        config.polyChildren()
                .change(c -> c.delete("0"))
                .get(1, SECONDS);

        assertTrue(invokeListener.get());
    }

    @Test
    void testNotificationEventForNestedConfigAfterNotifyListeners() throws Exception {
        AtomicReference<ConfigurationNotificationEvent<?>> eventRef = new AtomicReference<>();

        config.child().listen(configListener(eventRef::set));

        config.child().str().update(UUID.randomUUID().toString()).get(1, SECONDS);

        ConfigurationNotificationEvent<?> event = eventRef.get();

        assertNotNull(event);

        assertInstanceOf(ChildView.class, event.newValue());
        assertInstanceOf(InternalChildView.class, event.newValue());

        assertInstanceOf(ChildView.class, event.oldValue());
        assertInstanceOf(InternalChildView.class, event.oldValue());

        assertInstanceOf(ChildConfiguration.class, event.config(ChildConfiguration.class));
        assertInstanceOf(InternalChildConfiguration.class, event.config(InternalChildConfiguration.class));

        assertInstanceOf(ParentConfiguration.class, event.config(ParentConfiguration.class));

        assertNull(event.name(ChildConfiguration.class));
        assertNull(event.name(InternalChildConfiguration.class));
    }

    @Test
    void testNotificationEventForNamedConfigAfterNotifyListeners() throws Exception {
        AtomicReference<ConfigurationNotificationEvent<?>> eventRef = new AtomicReference<>();

        config.children().listenElements(configNamedListenerOnCreate(eventRef::set));

        config.children().change(c -> c.create("0", doNothingConsumer())).get(1, SECONDS);

        ConfigurationNotificationEvent<?> event = eventRef.get();

        assertNotNull(event);

        assertInstanceOf(ChildView.class, event.newValue());
        assertInstanceOf(InternalChildView.class, event.newValue());

        assertNull(event.oldValue());

        assertInstanceOf(ChildConfiguration.class, event.config(ChildConfiguration.class));
        assertInstanceOf(InternalChildConfiguration.class, event.config(InternalChildConfiguration.class));

        assertInstanceOf(ParentConfiguration.class, event.config(ParentConfiguration.class));

        assertEquals("0", event.name(ChildConfiguration.class));
        assertEquals("0", event.name(InternalChildConfiguration.class));
    }

    @Test
    void testGetErrorFromListener() {
        config.child().listen(configListener(ctx -> {
            throw new RuntimeException("from test");
        }));

        ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> config.child().str().update(UUID.randomUUID().toString()).get(1, SECONDS)
        );

        assertTrue(hasCause(ex, RuntimeException.class, "from test"));
    }

    @Test
    void testGetErrorFromListenerFuture() {
        config.child().listen(ctx -> CompletableFuture.failedFuture(new RuntimeException("from test")));

        ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> config.child().str().update(UUID.randomUUID().toString()).get(1, SECONDS)
        );

        assertTrue(hasCause(ex, RuntimeException.class, "from test"));
    }

    @Test
    void testNotifyListenersOnCurrentConfigWithoutChange() throws Exception {
        config.children().change(c -> c.create("0", doNothingConsumer())).get(1, SECONDS);

        config.polyChildren().change(c -> c.create("0", doNothingConsumer())).get(1, SECONDS);

        List<String> events = new ArrayList<>();

        config.listen(configListener(ctx -> events.add("root")));

        config.child().listen(configListener(ctx -> events.add("child")));
        config.child().str().listen(configListener(ctx -> events.add("child.str")));

        config.children().listen(configListener(ctx -> events.add("children")));
        config.children().listenElements(configNamedListenerOnCreate(ctx -> events.add("children.onCreate")));
        config.children().listenElements(configNamedListenerOnUpdate(ctx -> events.add("children.onUpdate")));
        config.children().listenElements(configNamedListenerOnRename(ctx -> events.add("children.onRename")));
        config.children().listenElements(configNamedListenerOnDelete(ctx -> events.add("children.onDelete")));

        config.children().get("0").listen(configListener(ctx -> events.add("children.0")));
        config.children().get("0").str().listen(configListener(ctx -> events.add("children.0.str")));

        config.children().any().listen(configListener(ctx -> events.add("children.any")));
        config.children().any().str().listen(configListener(ctx -> events.add("children.any.str")));

        // Polymorphic configs.

        config.polyChild().listen(configListener(ctx -> events.add("polyChild")));
        config.polyChild().commonIntVal().listen(configListener(ctx -> events.add("polyChild.int")));
        ((StringPolyConfiguration) config.polyChild()).specificVal().listen(configListener(ctx -> events.add("polyChild.str")));

        config.polyChildren().listen(configListener(ctx -> events.add("polyChildren")));
        config.polyChildren().listenElements(configNamedListenerOnCreate(ctx -> events.add("polyChildren.onCreate")));
        config.polyChildren().listenElements(configNamedListenerOnUpdate(ctx -> events.add("polyChildren.onUpdate")));
        config.polyChildren().listenElements(configNamedListenerOnRename(ctx -> events.add("polyChildren.onRename")));
        config.polyChildren().listenElements(configNamedListenerOnDelete(ctx -> events.add("polyChildren.onDelete")));

        config.polyChildren().get("0").listen(configListener(ctx -> events.add("polyChildren.0")));
        config.polyChildren().get("0").commonIntVal().listen(configListener(ctx -> events.add("polyChildren.0.int")));
        ((StringPolyConfiguration) config.polyChildren().get("0")).specificVal()
                .listen(configListener(ctx -> events.add("polyChildren.0.str")));

        config.polyChildren().any().listen(configListener(ctx -> events.add("polyChildren.any")));
        config.polyChildren().any().commonIntVal().listen(configListener(ctx -> events.add("polyChildren.any.int")));

        Collection<CompletableFuture<?>> futs = notifyListeners(
                null,
                (InnerNode) config.value(),
                (DynamicConfiguration) config,
                0
        );

        for (CompletableFuture<?> fut : futs) {
            fut.get(1, SECONDS);
        }

        assertEquals(
                List.of(
                        "root",
                        "child", "child.str",
                        "children", "children.onCreate", "children.any", "children.0",
                        "children.any.str", "children.0.str",
                        "polyChild", "polyChild.int", "polyChild.str",
                        "polyChildren", "polyChildren.onCreate", "polyChildren.any", "polyChildren.0",
                        "polyChildren.any.int", "polyChildren.0.int", "polyChildren.0.str"
                ),
                events
        );
    }

    @Test
    void testNotifyCurrentConfigurationListeners() throws Exception {
        AtomicBoolean invokeListener = new AtomicBoolean();

        config.listen(configListener(ctx -> {
            invokeListener.set(true);

            assertNull(ctx.oldValue());
            assertNotNull(ctx.newValue());
        }));

        registry.notifyCurrentConfigurationListeners().get(1, SECONDS);

        assertTrue(invokeListener.get());
    }
}
