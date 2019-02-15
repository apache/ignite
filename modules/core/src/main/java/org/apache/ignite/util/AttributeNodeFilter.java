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

package org.apache.ignite.util;

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.ServiceConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@code IgnitePredicate<ClusterNode>} based on
 * {@link IgniteConfiguration#getUserAttributes() user attributes}.
 * This filter can be used in methods like {@link ClusterGroup#forPredicate(IgnitePredicate)},
 * {@link CacheConfiguration#setNodeFilter(IgnitePredicate)},
 * {@link ServiceConfiguration#setNodeFilter(IgnitePredicate)}, etc.
 * <p>
 * The filter will evaluate to true if a node has <b>all</b> provided attributes set to
 * corresponding values. Here is an example of how you can configure node filter for a
 * cache or a service so that it's deployed only on nodes that have {@code group}
 * attribute set to value {@code data}:
 * <pre name="code" class="xml">
 * &lt;property name=&quot;nodeFilter&quot;&gt;
 *     &lt;bean class=&quot;org.apache.ignite.util.ClusterAttributeNodeFilter&quot;&gt;
 *         &lt;constructor-arg value="group"/&gt;
 *         &lt;constructor-arg value="data"/&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 * You can also specify multiple attributes for the filter:
 * <pre name="code" class="xml">
 * &lt;property name=&quot;nodeFilter&quot;&gt;
 *     &lt;bean class=&quot;org.apache.ignite.util.ClusterAttributeNodeFilter&quot;&gt;
 *         &lt;constructor-arg&gt;
 *             &lt;map&gt;
 *                 &lt;entry key=&quot;cpu-group&quot; value=&quot;high&quot;/&gt;
 *                 &lt;entry key=&quot;memory-group&quot; value=&quot;high&quot;/&gt;
 *             &lt;/map&gt;
 *         &lt;/constructor-arg&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 * With this configuration a cache or a service will deploy only on nodes that have both
 * {@code cpu-group} and {@code memory-group} attributes set to value {@code high}.
 */
public class AttributeNodeFilter implements IgnitePredicate<ClusterNode> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Attributes. */
    private final Map<String, Object> attrs;

    /**
     * Creates new node filter with a single attribute value.
     *
     * @param attrName Attribute name.
     * @param attrVal Attribute value.
     */
    public AttributeNodeFilter(String attrName, @Nullable Object attrVal) {
        A.notNull(attrName, "attrName");

        attrs = Collections.singletonMap(attrName, attrVal);
    }

    /**
     * Creates new node filter with a set of attributes.
     *
     * @param attrs Attributes.
     */
    public AttributeNodeFilter(Map<String, Object> attrs) {
        A.notNull(attrs, "attrs");

        this.attrs = attrs;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(ClusterNode node) {
        Map<String, Object> nodeAttrs = node.attributes();

        for (Map.Entry<String, Object> attr : attrs.entrySet()) {
            if (!F.eq(nodeAttrs.get(attr.getKey()), attr.getValue()))
                return false;
        }

        return true;
    }
}
