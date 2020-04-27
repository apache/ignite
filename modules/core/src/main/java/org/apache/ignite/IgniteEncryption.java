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

package org.apache.ignite;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.encryption.EncryptionSpi;

/**
 * Defines encryption features.
 * <p>
 * Ignite provides Transparent Data Encryption of cache data on disk. Encryption features are provided by {@link
 * EncryptionSpi} and can be configured via {@link IgniteConfiguration#setEncryptionSpi(EncryptionSpi)}. Cache
 * encryption can be enabled via {@link CacheConfiguration#setEncryptionEnabled(boolean)}.
 * <p>
 * Instance of {@code IgniteEncryption} is obtained from {@link Ignite} as follows:
 * <pre class="brush:java">
 * Ignite ignite = Ignition.ignite();
 *
 * IgniteEncryption encryption = ignite.encryption();
 * </pre>
 * Two types of keys are involved in data encryption: group and master keys.
 * <p>
 * Group key encrypts data of cache group caches. Each group key is encrypted by the master key. Encrypted group key
 * and encrypted data are written to disk.
 * <p>
 * Ignite provides the ability to change the master key. Master keys are identified by a name (see {@link
 * EncryptionSpi#getMasterKeyName()}). Follow operations are available for master key:
 * <ul>
 * <li>Method {@link #getMasterKeyName()} gets current master key name in the cluster.</li>
 * <li>Method {@link #changeMasterKey(String)} ()} starts master key change process.</li>
 * </ul>
 */
public interface IgniteEncryption {
    /**
     * Gets the current master key name.
     *
     * @return Master key name.
     */
    public String getMasterKeyName();

    /**
     * Starts master key change process.
     * <p>
     * Each node will re-encrypt group keys stored on the disk.
     * <p>
     * <b>NOTE:</b> The new master key should be available to {@link EncryptionSpi} for each server node. Cache start
     * and node join during the key change process is prohibited and will be rejected.
     * <p>
     * If some node was unavailable during a master key change process it won't be able to join to cluster with the old
     * master key. The node should re-encrypt group keys during recovery on startup. The actual master key
     * name should be set via {@link IgniteSystemProperties#IGNITE_MASTER_KEY_NAME_TO_CHANGE_BEFORE_STARTUP}.
     *
     * @return Future for this operation.
     */
    public IgniteFuture<Void> changeMasterKey(String masterKeyName);
}
