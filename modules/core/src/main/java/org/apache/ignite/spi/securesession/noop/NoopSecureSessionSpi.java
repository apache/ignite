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

package org.apache.ignite.spi.securesession.noop;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.securesession.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Default no-op implementation of the secure session SPI which supports all subject types and denies any token.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * This SPI has no optional configuration parameters.
 * <h2 class="header">Java Example</h2>
 * GridNoopSecureSessionSpi is used by default and has no parameters to be explicitly configured.
 * <pre name="code" class="java">
 * GridNoopSecureSessionSpi spi = new GridNoopSecureSessionSpi();
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override default SecureSession SPI.
 * cfg.setSecureSessionSpi(spi);
 *
 * // Start grid.
 * Ignition.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridNoopSecureSessionSpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.apache.ignite.configuration.IgniteConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="secureSessionSpi"&gt;
 *             &lt;bean class="org.apache.ignite.spi.SecureSession.noop.GridNoopSecureSessionSpi"/&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see org.apache.ignite.spi.securesession.SecureSessionSpi
 */
@IgniteSpiNoop
@IgniteSpiMultipleInstancesSupport(true)
public class NoopSecureSessionSpi extends IgniteSpiAdapter
    implements SecureSessionSpi, NoopSecureSessionSpiMBean {
    /** Empty bytes array. */
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /** Injected grid logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public boolean supported(GridSecuritySubjectType subjType) {
        // If this SPI is included, then session management is disabled.
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean validate(GridSecuritySubjectType subjType, UUID subjId, @Nullable byte[] tok,
        @Nullable Object params) throws IgniteSpiException {
        // Never validate any token - all tokens are invalid.
        return false;
    }

    /** {@inheritDoc} */
    @Override public byte[] generateSessionToken(GridSecuritySubjectType subjType, UUID subjId,
        @Nullable Object params) {
        return EMPTY_BYTE_ARRAY;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws IgniteSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        registerMBean(gridName, this, NoopSecureSessionSpiMBean.class);

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
    @Override public String toString() {
        return S.toString(NoopSecureSessionSpi.class, this);
    }
}
