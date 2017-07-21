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

package org.apache.ignite.internal.managers.deployment;

import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Test class deployment.
 */
public class GridTestDeployment extends GridDeployment {
    /**
     * @param depMode Deployment mode.
     * @param clsLdr Class loader.
     * @param clsLdrId Class loader ID.
     * @param userVer User version.
     * @param sampleClsName Sample class name.
     * @param loc {@code True} if local deployment.
     */
    public GridTestDeployment(DeploymentMode depMode, ClassLoader clsLdr, IgniteUuid clsLdrId,
        String userVer, String sampleClsName, boolean loc) {
        super(depMode, clsLdr, clsLdrId, userVer, sampleClsName, loc);
    }
}