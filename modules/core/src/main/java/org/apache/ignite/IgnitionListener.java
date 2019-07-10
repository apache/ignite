/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite;

import java.util.EventListener;

/**
 * Listener for gird state change notifications. Use
 * {@link Ignition#addListener(IgnitionListener)} to register this
 * listener with grid factory.
 */
public interface IgnitionListener extends EventListener {
    /**
     * Listener for grid factory state change notifications.
     *
     * @param name Ignite instance name ({@code null} for default un-named Ignite instance).
     * @param state New state.
     */
    public void onStateChange(String name, IgniteState state);
}