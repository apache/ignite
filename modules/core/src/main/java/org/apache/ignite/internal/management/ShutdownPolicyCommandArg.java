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

package org.apache.ignite.internal.management;

import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.Positional;

/** */
public class ShutdownPolicyCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Order(0)
    @Positional
    @Argument(optional = true)
    @EnumDescription(
        names = {
            "IMMEDIATE",
            "GRACEFUL"
        },
        descriptions = {
            "Stop immediately as soon as all components are ready",
            "Node will stop if and only if it does not store any unique partitions, that don't have another copies in the cluster"
        }
    )
    ShutdownPolicy shutdownPolicy;

    /** */
    public ShutdownPolicy shutdownPolicy() {
        return shutdownPolicy;
    }

    /** */
    public void shutdownPolicy(ShutdownPolicy shutdownPolicy) {
        this.shutdownPolicy = shutdownPolicy;
    }
}
