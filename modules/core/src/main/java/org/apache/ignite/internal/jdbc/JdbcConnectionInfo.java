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

package org.apache.ignite.internal.jdbc;

/**
 * Connection properties.
 */
public class JdbcConnectionInfo {
    /** URL. */
    private final String url;

    /** Hostname. */
    private String hostname;

    /** Port number. */
    private int port;

    /** Cache name. */
    private String cacheName;

    /**
     * @param url URL.
     */
    JdbcConnectionInfo(String url) {
        this.url = url;
    }

    /**
     * @return URL.
     */
    public String url() {
        return url;
    }

    /**
     * @return Hostname.
     */
    public String hostname() {
        return hostname;
    }

    /**
     * @param hostname Hostname.
     */
    public void hostname(String hostname) {
        this.hostname = hostname;
    }

    /**
     * @return Port number.
     */
    public int port() {
        return port;
    }

    /**
     * @param port Port number.
     */
    public void port(int port) {
        this.port = port;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }
}