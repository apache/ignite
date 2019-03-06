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

package org.apache.ignite.ml.math.primitives;

/**
 * Collect constants for org.apache.ignite.math tests
 */
public interface MathTestConstants {
    /** */
    public double SECOND_ARG = 1d;

    /**
     * We assume that we will check calculation precision in other tests.
     */
    public double EXP_DELTA = 0.1d;

    /** */
    public String UNEXPECTED_VAL = "Unexpected value.";

    /** */
    public String NULL_GUID = "Null GUID.";

    /** */
    public String UNEXPECTED_GUID_VAL = "Unexpected GUID value.";

    /** */
    public String EMPTY_GUID = "Empty GUID.";

    /** */
    public String VALUES_SHOULD_BE_NOT_EQUALS = "Values should be not equals.";

    /** */
    public String NULL_VAL = "Null value.";

    /** */
    public String NULL_VALUES = "Null values.";

    /** */
    public String NOT_NULL_VAL = "Not null value.";

    /** */
    public double TEST_VAL = 1d;

    /** */
    public String VAL_NOT_EQUALS = "Values not equals.";

    /** */
    public String NO_NEXT_ELEMENT = "No next element.";

    /** */
    public int STORAGE_SIZE = 100;

    /** */
    public String WRONG_ATTRIBUTE_VAL = "Wrong attribute value.";

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
    public String UNEXPECTED_DATA_VAL = "Unexpected data value.";
}
