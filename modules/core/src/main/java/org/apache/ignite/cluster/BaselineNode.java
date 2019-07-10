/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cluster;

import java.util.Map;
import org.apache.ignite.configuration.IgniteConfiguration;

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
     * @return Attribute value or {@code null} if such an attribute does not exist.
     */
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
