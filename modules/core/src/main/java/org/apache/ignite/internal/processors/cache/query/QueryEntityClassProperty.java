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

package org.apache.ignite.internal.processors.cache.query;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Description of type property.
 */
public class QueryEntityClassProperty {
    /** */
    private final Member member;

    /** */
    private QueryEntityClassProperty parent;

    /** */
    private String name;

    /** */
    private String alias;

    /**
     * Constructor.
     *
     * @param member Element.
     */
    public QueryEntityClassProperty(Member member) {
        this.member = member;

        name = member.getName();

        if (member instanceof Method) {
            if (member.getName().startsWith("get") && member.getName().length() > 3)
                name = member.getName().substring(3);

            if (member.getName().startsWith("is") && member.getName().length() > 2)
                name = member.getName().substring(2);
        }

        ((AccessibleObject)member).setAccessible(true);
    }

    /**
     * @param alias Alias.
     */
    public void alias(String alias) {
        this.alias = alias;
    }

    /**
     * @return Alias.
     */
    public String alias() {
        return F.isEmpty(alias) ? name : alias;
    }

    /**
     * @return Type.
     */
    public Class<?> type() {
        return member instanceof Field ? ((Field)member).getType() : ((Method)member).getReturnType();
    }

    /**
     * @param parent Parent property if this is embeddable element.
     */
    public void parent(QueryEntityClassProperty parent) {
        this.parent = parent;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryEntityClassProperty.class, this);
    }

    /**
     * @param cls Class.
     * @return {@code true} If this property or some parent relates to member of the given class.
     */
    public boolean knowsClass(Class<?> cls) {
        return member.getDeclaringClass() == cls || (parent != null && parent.knowsClass(cls));
    }

    /**
     * @return Full name with all parents in dot notation.
     */
    public String fullName() {
        assert name != null;

        if (parent == null)
            return name;

        return parent.fullName() + '.' + name;
    }
}
