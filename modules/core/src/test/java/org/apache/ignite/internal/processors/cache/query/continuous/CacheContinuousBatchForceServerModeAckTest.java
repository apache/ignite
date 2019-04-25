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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Serializable;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Continuous queries tests.
 */
public class CacheContinuousBatchForceServerModeAckTest extends CacheContinuousBatchAckTest implements Serializable {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.endsWith(CLIENT)) {
            cfg.setClientMode(true);

            FailedTcpCommunicationSpi spi = new FailedTcpCommunicationSpi(true, false);

            cfg.setCommunicationSpi(spi);
        }
        else if (igniteInstanceName.endsWith(SERVER2))
            cfg.setCommunicationSpi(new FailedTcpCommunicationSpi(false, true));

        else
            cfg.setCommunicationSpi(new FailedTcpCommunicationSpi(false, false));

        return cfg;
    }
}
