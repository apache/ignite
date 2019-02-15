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

package org.apache.ignite.internal.util.lang;

import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Adapter for common interfaces in closures, reducers and predicates.
 */
public class GridPeerDeployAwareAdapter implements GridPeerDeployAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Peer deploy aware class. */
    protected transient GridPeerDeployAware pda;

    /**
     * Sets object that from which peer deployment information
     * will be copied, i.e. this lambda object will be peer deployed
     * using the same class loader as given object.
     * <p>
     * Note that in most cases Ignite attempts to automatically call this
     * method whenever lambda classes like closures and predicates are created that
     * wrap user object (the peer deploy information in such cases will be copied
     * from the user object).
     * <p>
     * In general, if user gets class not found exception during peer loading it is
     * very likely that peer deploy information was lost during wrapping of one object
     * into another.
     *
     * @param obj Peer deploy aware.
     */
    public void peerDeployLike(Object obj) {
        assert obj != null;

        pda = U.peerDeployAware(obj);
    }

    /** {@inheritDoc} */
    @Override public Class<?> deployClass() {
        if (pda == null)
            pda = U.detectPeerDeployAware(this);

        return pda.deployClass();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader classLoader() {
        if (pda == null)
            pda = U.detectPeerDeployAware(this);

        return pda.classLoader();
    }
}