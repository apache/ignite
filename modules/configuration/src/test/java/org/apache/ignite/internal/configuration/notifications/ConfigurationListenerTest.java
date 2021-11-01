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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

/** */
public class ConfigurationListenerTest {
    /** */
    @ConfigurationRoot(rootName = "parent", type = LOCAL)
    public static class ParentConfigurationSchema {
        /** */
        @ConfigValue
        public ChildConfigurationSchema child;

        /** */
        @NamedConfigValue
        public ChildConfigurationSchema elements;
    }

    /** */
    @Config
    public static class ChildConfigurationSchema {
        /** */
        @Value(hasDefault = true)
        public String str = "default";
    }

    /** */
    private ConfigurationRegistry registry;

    /** */
    private ParentConfiguration configuration;

    /** */
    @BeforeEach
    public void before() {
        var testConfigurationStorage = new TestConfigurationStorage(LOCAL);

        registry = new ConfigurationRegistry(
            List.of(ParentConfiguration.KEY),
            Map.of(),
            testConfigurationStorage,
            List.of(),
            List.of()
        );

        registry.start();

        registry.initializeDefaults();

        configuration = registry.getConfiguration(ParentConfiguration.KEY);
    }

    /** */
    @AfterEach
    public void after() {
        registry.stop();
    }

    /** */
    @Test
    public void childNode() throws Exception {
        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            assertEquals(ctx.oldValue().child().str(), "default");
            assertEquals(ctx.newValue().child().str(), "foo");

            log.add("parent");

            return completedFuture(null);
        });

        configuration.child().listen(ctx -> {
            assertEquals(ctx.oldValue().str(), "default");
            assertEquals(ctx.newValue().str(), "foo");

            log.add("child");

            return completedFuture(null);
        });

        configuration.child().str().listen(ctx -> {
            assertEquals(ctx.oldValue(), "default");
            assertEquals(ctx.newValue(), "foo");

            log.add("str");

            return completedFuture(null);
        });

        configuration.elements().listen(ctx -> {
            log.add("elements");

            return completedFuture(null);
        });

        configuration.change(parent -> parent.changeChild(child -> child.changeStr("foo"))).get(1, SECONDS);

        assertEquals(List.of("parent", "child", "str"), log);
    }

    /**
     * Tests notifications validity when a new named list element is created.
     */
    @Test
    public void namedListNodeOnCreate() throws Exception {
        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        configuration.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        configuration.elements().listen(ctx -> {
            assertEquals(0, ctx.oldValue().size());

            ChildView newValue = ctx.newValue().get("name");

            assertNotNull(newValue);
            assertEquals("default", newValue.str());

            log.add("elements");

            return completedFuture(null);
        });

        configuration.elements().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                assertNull(ctx.oldValue());

                ChildView newValue = ctx.newValue();

                assertNotNull(newValue);
                assertEquals("default", newValue.str());

                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onRename(
                String oldName,
                String newName,
                ConfigurationNotificationEvent<ChildView> ctx
            ) {
                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return completedFuture(null);
            }
        });

        configuration.change(parent ->
            parent.changeElements(elements -> elements.create("name", element -> {}))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "create"), log);
    }

    /**
     * Tests notifications validity when a named list element is edited.
     */
    @Test
    public void namedListNodeOnUpdate() throws Exception {
        configuration.change(parent ->
            parent.changeElements(elements -> elements.create("name", element -> {}))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        configuration.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        configuration.elements().listen(ctx -> {

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            ChildView newValue = ctx.newValue().get("name");

            assertNotNull(newValue);
            assertEquals("foo", newValue.str());

            log.add("elements");

            return completedFuture(null);
        });

        configuration.elements().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
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
            @Override public CompletableFuture<?> onRename(
                String oldName,
                String newName,
                ConfigurationNotificationEvent<ChildView> ctx
            ) {
                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return completedFuture(null);
            }
        });

        configuration.change(parent ->
            parent.changeElements(elements -> elements.createOrUpdate("name", element -> element.changeStr("foo")))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "update"), log);
    }

    /**
     * Tests notifications validity when a named list element is renamed.
     */
    @Test
    public void namedListNodeOnRename() throws Exception {
        configuration.change(parent ->
            parent.changeElements(elements -> elements.create("name", element -> {}))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        configuration.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        configuration.elements().listen(ctx -> {
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

        configuration.elements().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onRename(
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
            @Override public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return completedFuture(null);
            }
        });

        configuration.change(parent ->
            parent.changeElements(elements -> elements.rename("name", "newName"))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "rename"), log);
    }

    /**
     * Tests notifications validity when a named list element is renamed and updated at the same time.
     */
    @Test
    public void namedListNodeOnRenameAndUpdate() throws Exception {
        configuration.change(parent ->
            parent.changeElements(elements -> elements.create("name", element -> {}))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        configuration.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        configuration.elements().listen(ctx -> {
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

        configuration.elements().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onRename(
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
            @Override public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return completedFuture(null);
            }
        });

        configuration.change(parent ->
            parent.changeElements(elements -> elements
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
        configuration.change(parent ->
            parent.changeElements(elements -> elements.create("name", element -> {}))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        configuration.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        configuration.elements().listen(ctx -> {
            assertEquals(0, ctx.newValue().size());

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            log.add("elements");

            return completedFuture(null);
        });

        configuration.elements().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onRename(
                String oldName,
                String newName,
                ConfigurationNotificationEvent<ChildView> ctx
            ) {
                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                assertNull(ctx.newValue());

                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                log.add("delete");

                return completedFuture(null);
            }
        });

        configuration.elements().get("name").listen(ctx -> {
            return completedFuture(null);
        });

        configuration.change(parent ->
            parent.changeElements(elements -> elements.delete("name"))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "delete"), log);
    }

    /**
     * Tests that concurrent configuration access does not affect configuration listeners.
     */
    @Test
    public void dataRace() throws Exception {
        configuration.change(parent -> parent.changeElements(elements ->
            elements.create("name", e -> {}))
        ).get(1, SECONDS);

        CountDownLatch wait = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            try {
                wait.await(1, SECONDS);
            }
            catch (InterruptedException e) {
                fail(e.getMessage());
            }

            release.countDown();

            return completedFuture(null);
        });

        configuration.elements().get("name").listen(ctx -> {
            assertNull(ctx.newValue());

            log.add("deleted");

            return completedFuture(null);
        });

        Future<Void> fut = configuration.change(parent -> parent.changeElements(elements ->
            elements.delete("name"))
        );

        wait.countDown();

        configuration.elements();

        release.await(1, SECONDS);

        fut.get(1, SECONDS);

        assertEquals(List.of("deleted"), log);
    }

    /** */
    @Test
    void testStopListen() throws Exception {
        List<String> events = new ArrayList<>();

        ConfigurationListener<ParentView> listener0 = configListener(ctx -> events.add("0"));
        ConfigurationListener<ParentView> listener1 = configListener(ctx -> events.add("1"));

        ConfigurationNamedListListener<ChildView> listener2 = configNamedListenerOnUpdate(ctx -> events.add("2"));
        ConfigurationNamedListListener<ChildView> listener3 = configNamedListenerOnUpdate(ctx -> events.add("3"));

        configuration.listen(listener0);
        configuration.listen(listener1);

        configuration.elements().listenElements(listener2);
        configuration.elements().listenElements(listener3);

        configuration.elements().change(c -> c.create("0", doNothingConsumer())).get(1, SECONDS);

        checkContainsListeners(
            () -> configuration.elements().get("0").str().update(UUID.randomUUID().toString()),
            events,
            List.of("0", "1", "2", "3"),
            List.of()
        );

        configuration.stopListen(listener0);
        configuration.elements().stopListenElements(listener2);

        checkContainsListeners(
            () -> configuration.elements().get("0").str().update(UUID.randomUUID().toString()),
            events,
            List.of("1", "3"),
            List.of("0", "2")
        );

        configuration.stopListen(listener1);
        configuration.elements().stopListenElements(listener3);

        checkContainsListeners(
            () -> configuration.elements().get("0").str().update(UUID.randomUUID().toString()),
            events,
            List.of(),
            List.of("0", "1", "2", "3")
        );
    }

    /** */
    @Test
    void testGetConfigFromNotificationEvent() throws Exception {
        String newVal = UUID.randomUUID().toString();

        configuration.listen(configListener(ctx -> {
            ParentConfiguration parent = ctx.config(ParentConfiguration.class);

            assertNotNull(parent);
            assertNull(ctx.name(ParentConfiguration.class));

            assertEquals(newVal, parent.child().str().value());
        }));

        configuration.child().listen(configListener(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));

            ChildConfiguration child = ctx.config(ChildConfiguration.class);

            assertNotNull(child);
            assertNull(ctx.name(ChildConfiguration.class));

            assertEquals(newVal, child.str().value());
        }));

        configuration.child().str().listen(configListener(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));

            ChildConfiguration child = ctx.config(ChildConfiguration.class);

            assertNotNull(child);
            assertNull(ctx.name(ChildConfiguration.class));

            assertEquals(newVal, child.str().value());
        }));

        configuration.change(c0 -> c0.changeChild(c1 -> c1.changeStr(newVal))).get(1, SECONDS);
    }

    /** */
    @Test
    void testGetConfigFromNotificationEventOnCreate() throws Exception {
        String newVal = UUID.randomUUID().toString();
        String key = UUID.randomUUID().toString();

        configuration.elements().listen(configListener(ctx -> {
            ParentConfiguration parent = ctx.config(ParentConfiguration.class);

            assertNotNull(parent);
            assertNull(ctx.name(ParentConfiguration.class));

            assertNull(ctx.config(ChildConfiguration.class));
            assertNull(ctx.name(ChildConfiguration.class));

            assertEquals(newVal, parent.elements().get(key).str().value());
        }));

        configuration.elements().listenElements(configNamedListenerOnCreate(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));
            assertNull(ctx.name(ParentConfiguration.class));

            ChildConfiguration child = ctx.config(ChildConfiguration.class);

            assertNotNull(child);
            assertEquals(key, ctx.name(ChildConfiguration.class));

            assertEquals(newVal, child.str().value());
        }));

        configuration.elements().change(c -> c.create(key, c1 -> c1.changeStr(newVal))).get(1, SECONDS);
    }

    /** */
    @Test
    void testGetConfigFromNotificationEventOnRename() throws Exception {
        String val = "default";
        String oldKey = UUID.randomUUID().toString();
        String newKey = UUID.randomUUID().toString();

        configuration.elements().change(c -> c.create(oldKey, doNothingConsumer())).get(1, SECONDS);

        configuration.elements().listen(configListener(ctx -> {
            ParentConfiguration parent = ctx.config(ParentConfiguration.class);

            assertNotNull(parent);
            assertNull(ctx.name(ParentConfiguration.class));

            assertNull(ctx.config(ChildConfiguration.class));
            assertNull(ctx.name(ChildConfiguration.class));

            assertNull(parent.elements().get(oldKey));
            assertEquals(val, parent.elements().get(newKey).str().value());
        }));

        configuration.elements().listenElements(configNamedListenerOnRename(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));
            assertNull(ctx.name(ParentConfiguration.class));

            ChildConfiguration child = ctx.config(ChildConfiguration.class);

            assertNotNull(child);
            assertEquals(newKey, ctx.name(ChildConfiguration.class));

            assertEquals(val, child.str().value());
        }));

        configuration.elements().change(c -> c.rename(oldKey, newKey));
    }

    /** */
    @Test
    void testGetConfigFromNotificationEventOnDelete() throws Exception {
        String key = UUID.randomUUID().toString();

        configuration.elements().change(c -> c.create(key, doNothingConsumer())).get(1, SECONDS);

        configuration.elements().listen(configListener(ctx -> {
            ParentConfiguration parent = ctx.config(ParentConfiguration.class);

            assertNotNull(parent);
            assertNull(ctx.name(ParentConfiguration.class));

            assertNull(ctx.config(ChildConfiguration.class));
            assertNull(ctx.name(ChildConfiguration.class));

            assertNull(parent.elements().get(key));
        }));

        configuration.elements().listenElements(configNamedListenerOnDelete(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));
            assertNull(ctx.name(ParentConfiguration.class));

            assertNull(ctx.config(ChildConfiguration.class));
            assertEquals(key, ctx.name(ChildConfiguration.class));
        }));

        configuration.elements().get(key).listen(configListener(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));
            assertNull(ctx.name(ParentConfiguration.class));

            assertNull(ctx.config(ChildConfiguration.class));
            assertEquals(key, ctx.name(ChildConfiguration.class));
        }));

        configuration.elements().change(c -> c.delete(key)).get(1, SECONDS);
    }

    /** */
    @Test
    void testGetConfigFromNotificationEventOnUpdate() throws Exception {
        String newVal = UUID.randomUUID().toString();
        String key = UUID.randomUUID().toString();

        configuration.elements().change(c -> c.create(key, doNothingConsumer())).get(1, SECONDS);

        configuration.elements().listen(configListener(ctx -> {
            ParentConfiguration parent = ctx.config(ParentConfiguration.class);

            assertNotNull(parent);
            assertNull(ctx.name(ParentConfiguration.class));

            assertNull(ctx.config(ChildConfiguration.class));
            assertNull(ctx.name(ChildConfiguration.class));

            assertEquals(newVal, parent.elements().get(key).str().value());
        }));

        configuration.elements().listenElements(configNamedListenerOnUpdate(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));
            assertNull(ctx.name(ParentConfiguration.class));

            ChildConfiguration child = ctx.config(ChildConfiguration.class);

            assertNotNull(child);
            assertEquals(key, ctx.name(ChildConfiguration.class));

            assertEquals(newVal, child.str().value());
        }));

        configuration.elements().get(key).listen(configListener(ctx -> {
            assertNotNull(ctx.config(ParentConfiguration.class));
            assertNull(ctx.name(ParentConfiguration.class));

            ChildConfiguration child = ctx.config(ChildConfiguration.class);

            assertNotNull(child);
            assertEquals(key, ctx.name(ChildConfiguration.class));

            assertEquals(newVal, child.str().value());
        }));

        configuration.elements().get(key).str().update(newVal).get(1, SECONDS);
    }
}
