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

package org.apache.ignite.ssl.jetty;

import org.apache.ignite.ssl.AbstractSslContextFactory;

/**
 * Hot reload settings of a Jetty SSL context factory that serves HTTPS for the HTTP REST endpoint. To change the settings
 * of a factory, implement this interface in a subclass of Jetty's {@code SslContextFactory.Server}, add setters for both
 * properties, and set them on the factory in the Jetty XML file. A factory that does not implement this interface keeps
 * hot reload enabled with the lead time {@link AbstractSslContextFactory#DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS}.
 * The reload command reloads the factory either way.
 */
public interface ReloadableJettySslFactory {
    /** @return Whether Ignite reloads the factory before its certificates expire. Changes require a node restart. */
    public boolean isHotReloadEnabled();

    /**
     * @return How long before the earliest certificate expiry in the private key chains hot reload becomes due,
     *     in milliseconds. Must be positive.
     */
    public long getHotReloadBeforeExpiryMs();
}
