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

package org.apache.ignite.math.impls;

/**
 * Collect constants for org.apache.ignite.math tests
 */
public interface MathTestConstants {
    /** */
    public double SECOND_ARG = 1d;

    /**
     * We assume that we will check calculation precision in other tests.
     */
    public double EXPECTED_DELTA = 0.1d;

    /** */
    public String UNEXPECTED_VALUE = "Unexpected value.";

    /** */
    public String NULL_GUID = "Null GUID.";

    /** */
    public String UNEXPECTED_GUID_VALUE = "Unexpected GUID value.";

    /** */
    public String EMPTY_GUID = "Empty GUID.";

    /** */
    public String VALUES_SHOULD_BE_NOT_EQUALS = "Values should be not equals.";

    /** */
    public String NULL_VALUE = "Null value.";

    /** */
    public String NULL_VALUES = "Null values.";

    /** */
    public String NOT_NULL_VALUE = "Not null value.";

    /** */
    public double TEST_VALUE = 1d;

    /** */
    public String VALUE_NOT_EQUALS = "Values not equals.";

    /** */
    public String NO_NEXT_ELEMENT = "No next element.";

    /** */
    public int STORAGE_SIZE = 100;

    /** */
    public String WRONG_ATTRIBUTE_VALUE = "Wrong attribute value.";

    /** */
    public String NULL_DATA_ELEMENT = "Null data element.";

    /** */
    public String WRONG_DATA_ELEMENT = "Wrong data element.";

    /** */
    public double NIL_DELTA = 0d;

    /** */
    public String NULL_DATA_STORAGE = "Null data storage.";

    /** */
    public String WRONG_DATA_SIZE = "Wrong data size.";

    /** */
    public String UNEXPECTED_DATA_VALUE = "Unexpected data value.";
}
