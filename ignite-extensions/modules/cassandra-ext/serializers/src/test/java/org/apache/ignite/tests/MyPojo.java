/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
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
