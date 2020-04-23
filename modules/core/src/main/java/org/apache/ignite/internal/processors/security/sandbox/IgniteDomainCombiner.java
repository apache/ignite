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

package org.apache.ignite.internal.processors.security.sandbox;

import java.security.DomainCombiner;
import java.security.PermissionCollection;
import java.security.ProtectionDomain;

/**
 * A {@code IgniteDomainCombiner} updates ProtectionDomains with passed {@code Permissions}.
 */
public class IgniteDomainCombiner implements DomainCombiner {
    /** */
    private final ProtectionDomain pd;

    /** */
    public IgniteDomainCombiner(PermissionCollection perms) {
        pd = new ProtectionDomain(null, perms);
    }

    /** {@inheritDoc} */
    @Override public ProtectionDomain[] combine(ProtectionDomain[] currDomains, ProtectionDomain[] assignedDomains) {
        if (currDomains == null || currDomains.length == 0)
            return assignedDomains;

        if (assignedDomains == null || assignedDomains.length == 0)
            return new ProtectionDomain[] {pd};

        ProtectionDomain[] res = new ProtectionDomain[assignedDomains.length + 1];

        res[0] = pd;

        System.arraycopy(assignedDomains, 0, res, 1, assignedDomains.length);

        return res;
    }
}
