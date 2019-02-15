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

package org.apache.ignite.cluster;

import java.util.Map;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Interface representing a single node from baseline topology.
 */
public interface BaselineNode {
    /**
     * Gets consistent globally unique node ID. This method returns consistent node ID which
     * survives node restarts.
     *
     * @return Consistent globally unique node ID.
     */
    public Object consistentId();

    /**
     * Gets a node attribute. Attributes are assigned to nodes at startup
     * via {@link IgniteConfiguration#getUserAttributes()} method.
     * <p>
     * The system adds the following attributes automatically:
     * <ul>
     * <li>{@code {@link System#getProperties()}} - All system properties.</li>
     * <li>{@code {@link System#getenv(String)}} - All environment properties.</li>
     * <li>All attributes defined in {@link org.apache.ignite.internal.IgniteNodeAttributes}</li>
     * </ul>
     * <p>
     * Note that attributes cannot be changed at runtime.
     *
     * @param <T> Attribute Type.
     * @param name Attribute name. <b>Note</b> that attribute names starting with
     *      {@code org.apache.ignite} are reserved for internal use.
     * @return Attribute value or {@code null}.
     */
    @Nullable
    public <T> T attribute(String name);

    /**
     * Gets all node attributes. Attributes are assigned to nodes at startup
     * via {@link IgniteConfiguration#getUserAttributes()} method.
     * <p>
     * The system adds the following attributes automatically:
     * <ul>
     * <li>{@code {@link System#getProperties()}} - All system properties.</li>
     * <li>{@code {@link System#getenv(String)}} - All environment properties.</li>
     * <li>All attributes defined in {@link org.apache.ignite.internal.IgniteNodeAttributes}</li>
     * </ul>
     * <p>
     * Note that attributes cannot be changed at runtime.
     *
     * @return All node attributes.
     */
    public Map<String, Object> attributes();
}
