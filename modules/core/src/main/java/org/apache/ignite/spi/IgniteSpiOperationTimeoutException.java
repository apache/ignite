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

package org.apache.ignite.spi;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/**
 * Kind of exception that is used when failure detection timeout is enabled for {@link TcpDiscoverySpi} or
 * {@link TcpCommunicationSpi}.
 *
 * For more information refer to {@link IgniteConfiguration#setFailureDetectionTimeout(long)} and
 * {@link IgniteSpiOperationTimeoutHelper}.
 */
public class IgniteSpiOperationTimeoutException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructor.
     * @param msg Error message.
     */
    public IgniteSpiOperationTimeoutException(String msg) {
        super(msg);
    }
}