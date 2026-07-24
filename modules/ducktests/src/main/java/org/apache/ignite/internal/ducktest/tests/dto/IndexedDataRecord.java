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

package org.apache.ignite.internal.ducktest.tests.dto;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** */
public class IndexedDataRecord {
    /** The base index converted to a string. */
    private final String strRepresentation;

    /** The primitive index boxed as an Integer. */
    private final Integer boxedIdx;

    /** True if the base index is an even number, false otherwise. */
    private final Boolean isEven;

    /** The first character of the string representation. */
    private final char leadingCharacter;

    /** An array containing the index, its double, and its square. */
    private final Integer[] seqArr;

    /** A list view of the sequence array. */
    private final List<Integer> seqList;

    /**
     * Constructs an immutable record by generating derived properties from the provided index.
     *
     * @param idx the base integer used to compute all internal fields
     */
    public IndexedDataRecord(int idx) {
        this.strRepresentation = String.valueOf(idx);
        this.boxedIdx = idx;
        this.isEven = (idx % 2 == 0);
        this.leadingCharacter = this.strRepresentation.charAt(0);
        this.seqArr = new Integer[] {idx, idx * 2, idx * idx};
        this.seqList = Arrays.asList(this.seqArr);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        IndexedDataRecord that = (IndexedDataRecord)o;

        return leadingCharacter == that.leadingCharacter
            && Objects.equals(strRepresentation, that.strRepresentation)
            && Objects.equals(boxedIdx, that.boxedIdx)
            && Objects.equals(isEven, that.isEven)
            && Arrays.equals(seqArr, that.seqArr)
            && Objects.equals(seqList, that.seqList);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(strRepresentation, boxedIdx, isEven, leadingCharacter, seqList);

        result = 31 * result + Arrays.hashCode(seqArr);

        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IndexedDataRecord{" +
            "stringRepresentation='" + strRepresentation + '\'' +
            ", boxedIndex=" + boxedIdx +
            ", isEven=" + isEven +
            ", leadingCharacter=" + leadingCharacter +
            ", sequenceArray=" + Arrays.toString(seqArr) +
            ", sequenceList=" + seqList +
            '}';
    }
}
