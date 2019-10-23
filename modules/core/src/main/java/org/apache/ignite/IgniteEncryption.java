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
import org.apache.ignite.spi.encryption.EncryptionSpi;

/**
 * Defines encryption processes functionality.
 * <p>
 * Ignite provides Transparent Data Encryption of cache data on disk. Encryption features is provided by {@link
 * EncryptionSpi} and can be configured via {@link IgniteConfiguration#setEncryptionSpi(EncryptionSpi)}. Cache
 * encryption is configured via {@link CacheConfiguration#setEncryptionEnabled(boolean)}.
 * <p>
 * Instance of {@code IgniteEncryption} is obtained from {@link Ignite} as follows:
 * <pre class="brush:java">
 * Ignite ignite = Ignition.ignite();
 *
 * IgniteEncryption encryption = ignite.encryption();
 * </pre>
 * Two types of keys are involved in data encryption: group and master keys.
 * <p>
 * Group key encrypts data of cache group caches. Each group key is encrypted by a master key. Encrypted group key and
 * encrypted data are written to disk.
 * <p>
 * Ignite provides ability to change master key. Master keys is identified by a master key name (see {@link
 * EncryptionSpi#getMasterKeyName()}). Follow operations are available for master keys:
 * <ul>
 * <li>Method {@link #getMasterKeyName()} gets current master key name in the cluster.</li>
 * <li>Method {@link #changeMasterKey(String)} ()} starts master key change process.</li>
 * </ul>
 */
public interface IgniteEncryption {
    /**
     * Gets current master key name.
     *
     * @return Master key name.
     */
    public String getMasterKeyName();

    /**
     * Starts master key change process.
     * <p>
     * Each node will re-encrypt group keys stored on the disk.
     * <p>
     * <b>NOTE:</b> New master key should be available to {@link EncryptionSpi} for each server node. Cache start and
     * node join during the key change process will be rejected.
     * <p>
     * If some node was unavailable during a master key change process it won't be able to join to cluster with old the
     * master key name. Node should re-encrypt group keys during the startup and recovery process. Set up valid master
     * key id via {@link IgniteSystemProperties#IGNITE_MASTER_KEY_NAME_TO_CHANGE_ON_STARTUP}.
     *
     * @throws IgniteException If the change master key process failed.
     */
    public void changeMasterKey(String masterKeyName);
}
