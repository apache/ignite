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

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.internal.util.nio.ssl.BlockingSslHandler;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;

/**
 * Copy of {@link GridTcpCommunicationSpiSslSelfTest}, but overriding initial buffer
 * sizes in {@link BlockingSslHandler}. This checks that {@code BUFFER_UNDERFLOW} and
 * {@code BUFFER_OVERFLOW} conditions are properly handled.
 */
@GridSpiTest(spi = TcpCommunicationSpi.class, group = "Communication SPI")
public class GridTcpCommunicationSpiSslSmallBuffersSelfTest extends GridTcpCommunicationSpiSslSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty("BlockingSslHandler.netBufSize", "1000");

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty("BlockingSslHandler.netBufSize");
    }
}
