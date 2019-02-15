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

package org.apache.ignite.marshaller;

import java.util.Arrays;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Grid marshaller test bean.
 */
public class GridMarshallerTestBean extends GridMarshallerResourceBean {
    /** */
    private Object objField;

    /** */
    private String strField;

    /** */
    private long longField = -1;

    /** */
    private GridByteArrayList buf;

    /** Array of classes. */
    private Class<?>[] clss;

    /** */
    private GridMarshallerExternalizableBean extBean = new GridMarshallerExternalizableBean();

    /**
     * @param objField Object field.
     * @param strField String field.
     * @param longField Long field.
     * @param buf Nested byte buffer.
     * @param clss Array of classes.
     */
    public GridMarshallerTestBean(Object objField, String strField, long longField, GridByteArrayList buf,
        Class<?>... clss) {
        this.objField = objField;
        this.strField = strField;
        this.longField = longField;
        this.buf = buf;
        this.clss = clss;
    }

    /**
     * Gets object field.
     *
     * @return Object field.
     */
    public Object getObjectField() {
        return objField;
    }

    /**
     * Gets string field.
     *
     * @return String field.
     */
    String getStringField() {
        return strField;
    }

    /**
     * Gets long field.
     *
     * @return Long field.
     */
    long getLongField() {
        return longField;
    }

    /**
     * Gets nested byte buffer.
     *
     * @return Nested byte buffer.
     */
    GridByteArrayList getBuffer() {
        return buf;
    }

    /**
     * Gets externalizable object.
     *
     * @return Externalizable object.
     */
    GridMarshallerExternalizableBean getExternalizableBean() {
        return extBean;
    }

    /**
     * @return Array of classes.
     */
    public Class<?>[] getClasses() {
        return clss;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int prime = 31;

        int res = 1;

        res = prime * res + ((buf == null) ? 0 : buf.hashCode());
        res = prime * res + (int) (longField ^ (longField >>> 32));
        res = prime * res + ((objField == null) ? 0 : objField.hashCode());
        res = prime * res + ((strField == null) ? 0 : strField.hashCode());

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        assert obj instanceof GridMarshallerTestBean;

        GridMarshallerTestBean other = (GridMarshallerTestBean)obj;

        return
            other.strField.equals(strField) &&
            other.longField == longField &&
            other.extBean.equals(extBean) &&
            Arrays.equals(other.buf.array(), buf.array()) &&
            other.buf.size() == buf.size() &&
            Arrays.equals(other.clss, clss);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridMarshallerTestBean.class, this);
    }
}