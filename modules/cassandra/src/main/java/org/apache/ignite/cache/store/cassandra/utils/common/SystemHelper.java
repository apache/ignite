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

package org.apache.ignite.cache.store.cassandra.utils.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Helper class providing system information about the host (ip, hostname, os and etc.)
 */
public class SystemHelper {
    public static final int PROCESSORS_COUNT = Runtime.getRuntime().availableProcessors();
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");
    public static final String DOUBLE_LINE_SEPARATOR = LINE_SEPARATOR + LINE_SEPARATOR;
    public static final String OS_NAME = System.getProperty("os.name");
    public static final String OS_USER = System.getProperty("user.name");
    public static final boolean IS_WINDOWS = OS_NAME.toLowerCase().contains("win");
    public static final boolean IS_MAC = OS_NAME.toLowerCase().contains("mac");
    public static final boolean IS_UNIX = OS_NAME.toLowerCase().contains("nux");
    public static final String HOST_NAME;
    public static final String HOST_IP;

    static {
        try {
            InetAddress address = InetAddress.getLocalHost();
            HOST_NAME = address.getHostName();
            HOST_IP = address.getHostAddress();
        }
        catch (UnknownHostException e) {
            throw new IllegalStateException("Failed to get host/ip of current computer", e);
        }
    }
}
