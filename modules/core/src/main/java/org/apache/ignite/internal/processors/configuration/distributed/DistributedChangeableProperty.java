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

package org.apache.ignite.internal.processors.configuration.distributed;

import java.io.Serializable;
import org.jetbrains.annotations.NotNull;

/**
 * Inner interface to maintenance of distributed property.
 */
public interface DistributedChangeableProperty<T extends Serializable> extends DistributedProperty<T> {
    /**
     * This property have been attached to processor.
     */
    void onAttached();

    /**
     * On this property ready to be update on cluster wide.
     *
     * @param updater Consumer for update value across cluster.
     */
    void onReadyForUpdate(@NotNull PropertyUpdateClosure updater);

    /**
     * Update only local value without updating remote cluster.
     *
     * @param newVal New value.
     */
    void localUpdate(Serializable newVal);
}
