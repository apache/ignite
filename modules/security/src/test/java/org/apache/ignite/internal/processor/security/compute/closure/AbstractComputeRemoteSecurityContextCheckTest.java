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

package org.apache.ignite.internal.processor.security.compute.closure;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractRemoteSecurityContextCheckTest;

/**
 * Abstract compute security test.
 */
public abstract class AbstractComputeRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** Name of client transition node. */
    public static final String CLNT_TRANSITION = "clnt_transition";

    /** Name of client endpoint node. */
    public static final String CLNT_ENDPOINT = "clnt_endpoint";

    /** {@inheritDoc} */
    @Override protected void startNodes() throws Exception{
        super.startNodes();

        startGrid(CLNT_TRANSITION, allowAllPermissionSet(), true);

        startGrid(CLNT_ENDPOINT, allowAllPermissionSet());
    }

    /**
     * Sets up VERIFIER, performs the runnable and checks the result.
     *
     * @param node Node.
     * @param r Runnable.
     */
    protected final void runAndCheck(IgniteEx node, Runnable r) {
        VERIFIER.start(secSubjectId(node))
            .add(SRV_TRANSITION, 1)
            .add(CLNT_TRANSITION, 1)
            .add(SRV_ENDPOINT, 2)
            .add(CLNT_ENDPOINT, 2);

        r.run();

        VERIFIER.checkResult();
    }

    /**
     * @return Collection of transition node ids.
     */
    protected Collection<UUID> transitions() {
        return Arrays.asList(
            grid(SRV_TRANSITION).localNode().id(),
            grid(CLNT_TRANSITION).localNode().id()
        );
    }

    /**
     * @return Collection of endpont nodes ids.
     */
    protected Collection<UUID> endpoints() {
        return Arrays.asList(
            grid(SRV_ENDPOINT).localNode().id(),
            grid(CLNT_ENDPOINT).localNode().id()
        );
    }
}