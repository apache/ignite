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

package org.springframework.boot.autoconfigure.ignite;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring boot starter configuration properties.
 */
@ConfigurationProperties(prefix = "spring.data.ignite")
public class IgniteProperties {
    /** Addresses to use for thin client connection. */
    private String[] clientAddresses;

    /**
     * @return Thin client connection address.
     */
    public String[] getClientAddresses() {
        return clientAddresses;
    }

    /**
     * Sets thin client connection address.
     *
     * @param clientAddresses Thin client connection address.
     */
    public void setClientAddresses(String[] clientAddresses) {
        this.clientAddresses = clientAddresses;
    }
}
