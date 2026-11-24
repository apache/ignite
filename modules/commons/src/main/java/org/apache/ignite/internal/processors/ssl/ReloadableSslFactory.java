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

package org.apache.ignite.internal.processors.ssl;

import org.apache.ignite.IgniteException;

/** TLS factory that the SSL factory registry can reload on request and before its certificates expire. */
public interface ReloadableSslFactory {
    /**
     * Loads the keys and trust settings again for new connections. If loading fails, new connections keep the previous ones.
     *
     * @throws IgniteException If loading fails.
     */
    public void init();

    /**
     * The hot reload time comes from the certificate chains of the private keys that new connections use.
     * Each certificate in these chains is due before it expires by a third of its lifetime, but by no more than 7 days.
     * The factory is due when its first certificate is due. Keys without an X.509 certificate chain are ignored,
     * as TLS cannot use them.
     *
     * @return Hot reload time in milliseconds since the epoch, or {@code 0} if the factory gives no private key with
     *     an X.509 certificate chain.
     * @throws IgniteException If the factory cannot give the certificates that new connections use.
     */
    public long getHotReloadTime();
}
