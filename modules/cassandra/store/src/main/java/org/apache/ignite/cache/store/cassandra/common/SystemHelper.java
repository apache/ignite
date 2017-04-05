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

package org.apache.ignite.cache.store.cassandra.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Helper class providing system information about the host (ip, hostname, os and etc.)
 */
public class SystemHelper {
    /** System line separator. */
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");

    /** Host name. */
    public static final String HOST_NAME;

    /** Host IP address */
    public static final String HOST_IP;

    static {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            HOST_NAME = addr.getHostName();
            HOST_IP = addr.getHostAddress();
        }
        catch (UnknownHostException e) {
            throw new IllegalStateException("Failed to get host/ip of current computer", e);
        }
    }
}
