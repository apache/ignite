/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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