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

package org.apache.ignite.internal.vault;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.vault.common.VaultEntry;

/**
 * VaultManager is responsible for handling VaultService lifecycle and providing interface for managing local keys.
 */
public class VaultManager {

    /**
     * @return {@code true} if VaultService beneath given VaultManager was bootstrapped with data
     * either from PDS or from user initial bootstrap configuration.
     */
    public boolean bootstrapped() {
        return false;
    }

    // TODO: IGNITE-14405 Local persistent key-value storage (Vault).

    /**
     * This is a proxy to Vault service method.
     */
    public CompletableFuture<VaultEntry> get(byte[] key) {
        return null;
    }
}
