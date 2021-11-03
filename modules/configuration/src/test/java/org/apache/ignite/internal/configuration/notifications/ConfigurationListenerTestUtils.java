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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.jetbrains.annotations.NotNull;

/**
 * Utility class for testing configuration listeners.
 */
class ConfigurationListenerTestUtils {
    /**
     * Private constructor.
     */
    private ConfigurationListenerTestUtils() {
    }

    /**
     * @return Consumer who does nothing.
     */
    static <T> Consumer<T> doNothingConsumer() {
        return t -> {
        };
    }

    /**
     * @param consumer Consumer of the notification context.
     * @return Config value change listener.
     */
    static <T> ConfigurationListener<T> configListener(Consumer<ConfigurationNotificationEvent<T>> consumer) {
        return ctx -> {
            try {
                consumer.accept(ctx);
            } catch (Throwable t) {
                return failedFuture(t);
            }

            return completedFuture(null);
        };
    }

    /**
     * Helper method for testing listeners.
     *
     * @param changeFun      Configuration change function.
     * @param events         Reference to the list of executing listeners that is filled after the {@code changeFun} is executed.
     * @param expContains    Listeners that are expected are contained in the {@code events}.
     * @param expNotContains Listeners that are expected are not contained in the {@code events}.
     * @throws Exception If failed.
     */
    static void checkContainsListeners(
            Supplier<CompletableFuture<Void>> changeFun,
            List<String> events,
            List<String> expContains,
            List<String> expNotContains
    ) throws Exception {
        events.clear();

        changeFun.get().get(1, SECONDS);

        for (String exp : expContains) {
            assertTrue(events.contains(exp), () -> exp + " not contains in " + events);
        }

        for (String exp : expNotContains) {
            assertFalse(events.contains(exp), () -> exp + " contains in " + events);
        }
    }

    /**
     * Helper method for testing listeners.
     *
     * @param changeFun Configuration change function.
     * @param exp       Expected list of executing listeners.
     * @param act       Reference to the list of executing listeners that is filled after the {@code changeFun} is executed.
     * @throws Exception If failed.
     */
    static void checkEqualsListeners(
            Supplier<CompletableFuture<Void>> changeFun,
            List<String> exp,
            List<String> act
    ) throws Exception {
        act.clear();

        changeFun.get().get(1, SECONDS);

        assertEquals(exp, act);
    }

    /**
     * @param consumer Consumer of the notification context.
     * @return Named config value change listener.
     */
    static <T> ConfigurationNamedListListener<T> configNamedListenerOnDelete(
            Consumer<ConfigurationNotificationEvent<T>> consumer
    ) {
        return new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override
            public @NotNull CompletableFuture<?> onDelete(@NotNull ConfigurationNotificationEvent<T> ctx) {
                try {
                    consumer.accept(ctx);
                } catch (Throwable t) {
                    return failedFuture(t);
                }

                return completedFuture(null);
            }
        };
    }

    /**
     * @param consumer Consumer of the notification context.
     * @return Named config value change listener.
     */
    static <T> ConfigurationNamedListListener<T> configNamedListenerOnRename(
            Consumer<ConfigurationNotificationEvent<T>> consumer
    ) {
        return new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override
            public @NotNull CompletableFuture<?> onRename(
                    @NotNull String oldName,
                    @NotNull String newName,
                    @NotNull ConfigurationNotificationEvent<T> ctx
            ) {
                try {
                    consumer.accept(ctx);
                } catch (Throwable t) {
                    return failedFuture(t);
                }

                return completedFuture(null);
            }
        };
    }

    /**
     * @param consumer Consumer of the notification context.
     * @return Named config value change listener.
     */
    static <T> ConfigurationNamedListListener<T> configNamedListenerOnCreate(
            Consumer<ConfigurationNotificationEvent<T>> consumer
    ) {
        return new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override
            public @NotNull CompletableFuture<?> onCreate(@NotNull ConfigurationNotificationEvent<T> ctx) {
                try {
                    consumer.accept(ctx);
                } catch (Throwable t) {
                    return failedFuture(t);
                }

                return completedFuture(null);
            }
        };
    }

    /**
     * @param consumer Consumer of the notification context.
     * @return Named config value change listener.
     */
    static <T> ConfigurationNamedListListener<T> configNamedListenerOnUpdate(
            Consumer<ConfigurationNotificationEvent<T>> consumer
    ) {
        return new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override
            public @NotNull CompletableFuture<?> onUpdate(@NotNull ConfigurationNotificationEvent<T> ctx) {
                try {
                    consumer.accept(ctx);
                } catch (Throwable t) {
                    return failedFuture(t);
                }

                return completedFuture(null);
            }
        };
    }
}
