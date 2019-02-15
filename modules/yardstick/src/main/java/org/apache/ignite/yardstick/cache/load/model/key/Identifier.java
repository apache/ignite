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

package org.apache.ignite.yardstick.cache.load.model.key;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Key cache class
 */
public class Identifier implements Comparable<Identifier>, Serializable {
    /**
     * Integer identifier
     */
    private int id;

    /**
     * String identifier
     */
    private String code;

    /**
     * Empty constructor
     */
    public Identifier() {
    }

    /**
     * @param id identifier
     * @param code code
     */
    public Identifier(int id, String code) {
        this.id = id;
        this.code = code;
    }

    /**
     * @return integer identifier
     */
    public int getId() {

        return id;
    }

    /**
     * @param id integer identifier
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return string identifier
     */
    public String getCode() {
        return code;
    }

    /**
     * @param code string identifier
     */
    public void setCode(String code) {
        this.code = code;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Identifier that = (Identifier)o;

        if (id != that.id)
            return false;

        return code != null ? code.equals(that.code) : that.code == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = id;
        res = 31 * res + (code != null ? code.hashCode() : 0);
        return res;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(Identifier o) {
        return Integer.compare(id, o.id);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Identifier.class, this);
    }
}
