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

package org.apache.ignite.internal;

import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;

public class TestDeploymentAwareMessage implements DeploymentAware {
    @Marshalled("dataBytes")
    Object data;

    @Order(0)
    byte[] dataBytes;

    @Marshalled(value = "keptBytes", keepBytes = true)
    Object kept;

    @Order(1)
    byte[] keptBytes;

    @Order(2)
    GridDeploymentInfoBean depInfo;

    @Order(3)
    String clsName;

    @Override public GridDeploymentInfo deploymentInfo() {
        return depInfo;
    }

    @Override public String deployedClassName() {
        return clsName;
    }

    public short directType() {
        return 0;
    }
}
