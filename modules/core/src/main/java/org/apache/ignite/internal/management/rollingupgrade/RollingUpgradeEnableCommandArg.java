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

package org.apache.ignite.internal.management.rollingupgrade;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Positional;

/** Rolling upgrade enable command argument. */
public class RollingUpgradeEnableCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** Target version. */
    @Order(value = 0)
    @Positional
    @Argument(description = "Target Ignite version. The target version can be one minor higher if its maintenance version is zero, "
        + "or one maintenance version higher (e.g. 2.18.0 -> 2.18.1 or 2.18.1 -> 2.19.0)")
    String targetVersion;

    /** Force flag. */
    @Order(value = 1)
    @Argument(description = "Enable rolling upgrade without target version checks."
        + " Use only when required, if the upgrade cannot proceed otherwise", optional = true)
    boolean force;

    /** */
    public String targetVersion() {
        return targetVersion;
    }

    /** */
    public void targetVersion(String targetVersion) {
        this.targetVersion = targetVersion;
    }

    /** */
    public boolean force() {
        return force;
    }

    /** */
    public void force(boolean force) {
        this.force = force;
    }
}
