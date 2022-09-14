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

package org.apache.ignite.springdata.proxy;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/** Represents Ignite cluster operations required by Spring Data. */
public interface IgniteProxy {
    /**
     * Gets existing cache with the given name or creates new one.
     *
     * @param <K> Key.
     * @param <V> Value.
     * @param name Cache name.
     * @return Cache proxy that provides access to cache with given name.
     */
    public <K, V> IgniteCacheProxy<K, V> getOrCreateCache(String name);

    /**
     * Gets cache with the given name.
     *
     * @param <K> Key.
     * @param <V> Value.
     * @param name Cache name.
     * @return Cache proxy that provides access to cache with specified name or {@code null} if it doesn't exist.
     */
    public <K, V> IgniteCacheProxy<K, V> cache(String name);

    /**
     * @param connObj Object that will be used to obtain underlying Ignite client instance to access the Ignite cluster.
     * @return Ignite proxy instance.
     */
    public static IgniteProxy of(Object connObj) {
        if (connObj instanceof Ignite)
            return new IgniteNodeProxy((Ignite)connObj);
        else if (connObj instanceof IgniteConfiguration) {
            try {
                return new IgniteNodeProxy(Ignition.ignite(((IgniteConfiguration)connObj).getIgniteInstanceName()));
            }
            catch (Exception ignored) {
                // No-op.
            }

            return new ClosableIgniteNodeProxy(Ignition.start((IgniteConfiguration)connObj));
        }
        else if (connObj instanceof String)
            return new ClosableIgniteNodeProxy(Ignition.start((String)connObj));
        else if (connObj instanceof IgniteClient)
            return new IgniteClientProxy((IgniteClient)connObj);
        else if (connObj instanceof ClientConfiguration)
            return new ClosableIgniteClientProxy(Ignition.startClient((ClientConfiguration)connObj));

        throw new IllegalArgumentException(
            "Object of type " + connObj.getClass().getName() + " can not be used to connect to the Ignite cluster.");
    }
}
