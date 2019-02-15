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

import java.io.Serializable;

/**
 * Represents any class that needs to maintain or carry on peer deployment information.
 * <p>
 * This interface is intended to be used primarily by Ignite's code.
 * User's code can however implement this interface, for example, if it wraps a
 * closure or a predicate and wants to maintain its peer deployment
 * information so that the user class could be peer-deployed as well.
 */
public interface GridPeerDeployAware extends Serializable {
    /**
     * Gets top level user class being deployed.
     *
     * @return Top level user deployed class.
     */
    public Class<?> deployClass();

    /**
     * Gets class loader for the class. This class loader must be able to load
     * the class returned from {@link #deployClass()} as well as all of its
     * dependencies.
     * <p>
     * Note that in most cases the class loader returned from this method
     * and the class loader for the class returned from {@link #deployClass()} method
     * will be the same. If they are not the same, it is required that the class loader
     * returned from this method still has to be able to load the deploy class and all its
     * dependencies.
     *
     * @return Class loader for the class.
     */
    public ClassLoader classLoader();
}