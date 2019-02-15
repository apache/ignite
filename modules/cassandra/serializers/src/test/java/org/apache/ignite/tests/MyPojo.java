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

package org.apache.ignite.tests;

import java.io.Serializable;
import java.util.Date;

/**
 * Sample POJO for tests.
 */
public class MyPojo implements Serializable {
    /** */
    private String field1;

    /** */
    private int field2;

    /** */
    private long field3;

    /** */
    private Date field4;

    /** */
    private MyPojo ref;

    /**
     * Empty constructor.
     */
    public MyPojo() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param field1 Some value.
     * @param field2 Some value.
     * @param field3 Some value.
     * @param field4 Some value.
     * @param ref Reference to other pojo.
     */
    public MyPojo(String field1, int field2, long field3, Date field4, MyPojo ref) {
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
        this.field4 = field4;
        this.ref = ref;
    }

    /**
     * Compare POJOs.
     *
     * @param obj POJO to compare with.
     * @return {@code true} if equals.
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof MyPojo))
            return false;

        MyPojo myObj = (MyPojo)obj;

        if ((field1 == null && myObj.field1 != null) ||
            (field1 != null && !field1.equals(myObj.field1)))
            return false;

        if ((field4 == null && myObj.field4 != null) ||
            (field4 != null && !field4.equals(myObj.field4)))
            return false;

        return field2 == myObj.field2 && field3 == myObj.field3;
    }

    /**
     * @param ref New reference.
     */
    public void setRef(MyPojo ref) {
        this.ref = ref;
    }

    /**
     * @return Reference to some POJO.
     */
    public MyPojo getRef() {
        return ref;
    }
}
