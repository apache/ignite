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

package org.apache.ignite.internal.processors.rollingupgrade;

import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteVersionUtils.semanticVersion;

/** */
class IgniteComponentUpgradeState {
    /** */
    final String cmpName;

    /** */
    @Nullable final IgniteProductVersion srcVer;

    /** */
    @Nullable final IgniteProductVersion targetVer;

    /** Whether all cluster nodes hold the same Ignite component version. */
    final boolean isClusterVerHomogenous;

    /** */
    IgniteComponentUpgradeState(
        String cmpName,
        @Nullable IgniteProductVersion srcVer,
        @Nullable IgniteProductVersion targetVer,
        boolean isClusterVerHomogenous
    ) {
        this.cmpName = cmpName;
        this.srcVer = srcVer;
        this.targetVer = targetVer;
        this.isClusterVerHomogenous = isClusterVerHomogenous;
    }

    /** */
    boolean isCompatible(IgniteProductVersion joiningNodeCmpVer) {
        if (isClusterVerHomogenous)
            return srcVer == null || srcVer.compareTo(joiningNodeCmpVer) <= 0;

        return joiningNodeCmpVer.equals(srcVer) || joiningNodeCmpVer.equals(targetVer);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder("[componentName=");

        sb.append(cmpName);

        sb.append(", compatibleComponentVersions=");

        if (isClusterVerHomogenous)
            if (srcVer == null)
                sb.append("[any]");
            else
                sb.append("[").append(semanticVersion(srcVer)).append(" or greater]");
        else {
            sb.append("[");

            if (srcVer != null)
                sb.append(srcVer);

            if (targetVer != null) {
                if (srcVer != null)
                    sb.append(", ");

                sb.append(targetVer);
            }

            sb.append("]");
        }

        sb.append("]");

        return sb.toString();
    }
}
