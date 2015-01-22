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

package org.apache.ignite.spi.authentication.noop;

import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.authentication.*;
import org.apache.ignite.internal.managers.security.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;

/**
 * Default implementation of the authentication SPI which permits any request.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * This SPI has no optional configuration parameters.
 * <h2 class="header">Java Example</h2>
 * GridNoopAuthenticationSpi is used by default and has no parameters to be explicitly configured.
 * <pre name="code" class="java">
 * GridNoopAuthenticationSpi authSpi = new GridNoopAuthenticationSpi();
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override default authentication SPI.
 * cfg.setAuthenticationSpi(authSpi);
 *
 * // Start grid.
 * GridGain.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridNoopAuthenticationSpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="authenticationSpi"&gt;
 *             &lt;bean class="org.gridgain.grid.spi.authentication.noop.GridNoopAuthenticationSpi"/&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
@IgniteSpiNoop
@IgniteSpiMultipleInstancesSupport(true)
public class NoopAuthenticationSpi extends IgniteSpiAdapter
    implements AuthenticationSpi, NoopAuthenticationSpiMBean {
    /** Injected grid logger. */
    @IgniteLoggerResource
    @GridToStringExclude
    private IgniteLogger log;

    /** Always allow permission set. */
    private static final GridSecurityPermissionSet allowAll = new GridAllowAllPermissionSet();

    /** {@inheritDoc} */
    @Override public boolean supported(GridSecuritySubjectType subjType) {
        // If this SPI is configured, then authentication is disabled.
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridSecuritySubject authenticate(AuthenticationContext authCtx) throws IgniteSpiException {
        GridSecuritySubjectAdapter subj = new GridSecuritySubjectAdapter(authCtx.subjectType(), authCtx.subjectId());

        subj.address(authCtx.address());
        subj.permissions(allowAll);

        if (authCtx.credentials() != null)
            subj.login(authCtx.credentials().getLogin());

        return subj;
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws IgniteSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        registerMBean(gridName, this, NoopAuthenticationSpiMBean.class);

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
        return S.toString(NoopAuthenticationSpi.class, this);
    }
}
