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

package org.apache.ignite.client;

/**
 * Ignite client configuration.
 * TODO: improve and finalize IGNITE-15164.
 */
public interface IgniteClientConfiguration {
    /** Default port. */
    int DFLT_PORT = 10800;

    /** Default port range. */
    int DFLT_PORT_RANGE = 100;

    /** Default socket send and receive buffer size. */
    int DFLT_SOCK_BUF_SIZE = 0;

    /** Default value for {@code TCP_NODELAY} socket option. */
    boolean DFLT_TCP_NO_DELAY = true;

    /**
     * Gets the address finder.
     *
     * @return Address finder.
     */
    IgniteClientAddressFinder getAddressesFinder();

    /**
     * Gets the addresses.
     *
     * @return Addresses.
     */
    String[] getAddresses();

    /**
     * Gets the retry limit.
     *
     * @return Retry limit.
     */
    int getRetryLimit();
}
