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

package org.apache.ignite.configuration.notifications;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.CompletableFuture;

/**
 * Configuration property change listener for named list configurations.
 *
 * @param <VIEWT> VIEW type configuration.
 */
public interface ConfigurationNamedListListener<VIEWT> extends ConfigurationListener<VIEWT> {
    /**
     * Called when new named list element is created.
     *
     * @param ctx Notification context.
     * @return Future that signifies the end of the listener execution.
     */
    default CompletableFuture<?> onCreate(ConfigurationNotificationEvent<VIEWT> ctx) {
        return completedFuture(null);
    }

    /**
     * Called when a named list element is renamed. Semantically equivalent to {@link #onUpdate(ConfigurationNotificationEvent)} with the
     * difference that the content of the element might have not been changed. No separate {@link #onUpdate(ConfigurationNotificationEvent)}
     * call is performed when {@link #onRename(String, String, ConfigurationNotificationEvent)} is already invoked.
     *
     * @param oldName Name, previously assigned to the element.
     * @param newName New name of the element.
     * @param ctx     Notification context.
     * @return Future that signifies the end of the listener execution.
     */
    default CompletableFuture<?> onRename(String oldName, String newName, ConfigurationNotificationEvent<VIEWT> ctx) {
        return completedFuture(null);
    }

    /**
     * Called when named list element is deleted.
     *
     * @param ctx Notification context.
     * @return Future that signifies the end of the listener execution.
     */
    default CompletableFuture<?> onDelete(ConfigurationNotificationEvent<VIEWT> ctx) {
        return completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override
    default CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<VIEWT> ctx) {
        return completedFuture(null);
    }
}
