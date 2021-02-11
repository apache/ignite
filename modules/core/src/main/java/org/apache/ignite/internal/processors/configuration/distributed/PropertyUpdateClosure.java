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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Closure of cluster wide update of distributed property.
 */
public interface PropertyUpdateClosure {

    /**
     * Update property on cluster.
     *
     * @param key Property key.
     * @param newValue New value.
     * @return Future this boolean value.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> update(String key, Serializable newValue) throws IgniteCheckedException;

    /**
     * Update property on cluster using compare and set way.
     *
     * @param key Property key.
     * @param expectedValue Expected value for CAS.
     * @param newValue New value.
     * @return Future this boolean value. * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> casUpdate(String key, Serializable expectedValue, Serializable newValue)
        throws IgniteCheckedException;
}
