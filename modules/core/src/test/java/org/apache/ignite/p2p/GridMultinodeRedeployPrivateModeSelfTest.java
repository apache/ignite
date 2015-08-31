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

package org.apache.ignite.p2p;

import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;

/**
 * Private deployment mode test.
 */
@GridCommonTest(group = "P2P")
public class GridMultinodeRedeployPrivateModeSelfTest extends GridAbstractMultinodeRedeployTest {
    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Throwable if error occur.
     */
    public void testPrivateMode() throws Throwable {
        processTest(PRIVATE);
    }
}