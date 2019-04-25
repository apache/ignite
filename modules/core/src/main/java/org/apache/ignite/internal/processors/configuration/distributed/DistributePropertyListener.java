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

package org.apache.ignite.internal.processors.configuration.distributed;

/**
 * Listener of distributed property change event.
 */
public interface DistributePropertyListener<T> {
    /**
     * Handle changed value event.
     *
     * @param name Name of distributed property.
     * @param oldVal Old value which was changed from.
     * @param newVal New value which was changed to.
     */
    void onUpdate(String name, T oldVal, T newVal);
}
