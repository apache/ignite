/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.deployment;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Simple adapter for {@link DeploymentResource} interface.
 */
public class DeploymentResourceAdapter implements DeploymentResource {
    /** */
    private final String name;

    /** */
    private final Class<?> rsrcCls;

    /** */
    private final ClassLoader clsLdr;

    /**
     * Creates resource.
     *
     * @param name Resource name.
     * @param rsrcCls Class.
     * @param clsLdr Class loader.
     */
    public DeploymentResourceAdapter(String name, Class<?> rsrcCls, ClassLoader clsLdr) {
        assert name != null;
        assert rsrcCls != null;
        assert clsLdr != null;

        this.name = name;
        this.rsrcCls = rsrcCls;
        this.clsLdr = clsLdr;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public Class<?> getResourceClass() {
        return rsrcCls;
    }

    /** {@inheritDoc} */
    @Override public ClassLoader getClassLoader() {
        return clsLdr;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        DeploymentResourceAdapter rsrc = (DeploymentResourceAdapter)obj;

        return clsLdr.equals(rsrc.clsLdr) == true && name.equals(rsrc.name) == true &&
            rsrcCls.equals(rsrc.rsrcCls) == true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = name.hashCode();

        res = 31 * res + rsrcCls.hashCode();
        res = 31 * res + clsLdr.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DeploymentResourceAdapter.class, this);
    }
}