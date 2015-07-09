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

package org.apache.ignite.tests.p2p.cache;


import java.io.*;
import java.util.*;

/**
 *
 */
@SuppressWarnings({"ReturnOfDateField", "AssignmentToDateFieldFromParameter"})
public class IndexValue implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int field1;

    /** */
    private String field2;

    /** */
    private Date field3;

    /** */
    private Object field4;

    /**
     * @return Field1 value.
     */
    public int getField1() {
        return field1;
    }

    /**
     * @param field1 Field1 value.
     */
    public void setField1(int field1) {
        this.field1 = field1;
    }

    /**
     * @return Field2 value.
     */
    public String getField2() {
        return field2;
    }

    /**
     * @param field2 Field2 value.
     */
    public void setField2(String field2) {
        this.field2 = field2;
    }

    /**
     * @return Field3 value.
     */
    public Date getField3() {
        return field3;
    }

    /**
     * @param field3 Field3 value.
     */
    public void setField3(Date field3) {
        this.field3 = field3;
    }

    /**
     * @return Field4 value.
     */
    public Object getField4() {
        return field4;
    }

    /**
     * @param field4 Field4 value.
     */
    public void setField4(Object field4) {
        this.field4 = field4;
    }
}
