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
