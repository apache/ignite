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

package org.apache.ignite.spi.failover.never;

import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.failover.FailoverContext;
import org.apache.ignite.spi.failover.FailoverSpi;

/**
 * This class provides failover SPI implementation that never fails over. This implementation
 * never fails over a failed job by always returning {@code null} out of
 * {@link org.apache.ignite.spi.failover.FailoverSpi#failover(org.apache.ignite.spi.failover.FailoverContext, List)} method.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * This SPI has no optional configuration parameters.
 * <p>
 * Here is a Java example on how to configure grid with {@code GridNeverFailoverSpi}:
 * <pre name="code" class="java">
 * NeverFailoverSpi spi = new NeverFailoverSpi();
 *
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * // Override default failover SPI.
 * cfg.setFailoverSpiSpi(spi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * Here is an example on how to configure grid with {@link NeverFailoverSpi} from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;property name="failoverSpi"&gt;
 *     &lt;bean class="org.apache.ignite.spi.failover.never.NeverFailoverSpi"/&gt;
 * &lt;/property&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see org.apache.ignite.spi.failover.FailoverSpi
 */
@IgniteSpiMultipleInstancesSupport(true)
public class NeverFailoverSpi extends IgniteSpiAdapter implements FailoverSpi, NeverFailoverSpiMBean {
    /** Injected grid logger. */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws IgniteSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        registerMBean(gridName, this, NeverFailoverSpiMBean.class);

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public ClusterNode failover(FailoverContext ctx, List<ClusterNode> top) {
        U.warn(log, "Returning 'null' node for failed job (failover will not happen) [job=" +
            ctx.getJobResult().getJob() + ", task=" +  ctx.getTaskSession().getTaskName() +
            ", sessionId=" + ctx.getTaskSession().getId() + ']');

        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NeverFailoverSpi.class, this);
    }
}