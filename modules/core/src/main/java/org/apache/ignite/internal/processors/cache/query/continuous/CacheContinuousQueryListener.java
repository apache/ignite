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

package org.apache.ignite.internal.processors.cache.query.continuous;

/**
 * Continuous query listener.
 */
interface CacheContinuousQueryListener<K, V> {
    /**
     * Query execution callback.
     */
    public void onExecution();

    /**
     * Entry update callback.
     *
     * @param evt Event
     * @param primary Primary flag.
     * @param recordIgniteEvt Whether to record event.
     */
    public void onEntryUpdated(CacheContinuousQueryEvent<K, V> evt, boolean primary, boolean recordIgniteEvt);

    /**
     * Listener unregistered callback.
     */
    public void onUnregister();

    /**
     * @return Whether old value is required.
     */
    public boolean oldValueRequired();

    /**
     * @return Whether to notify on existing entries.
     */
    public boolean notifyExisting();
}