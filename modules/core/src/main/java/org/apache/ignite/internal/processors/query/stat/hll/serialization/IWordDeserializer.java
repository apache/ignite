/*
 * Copyright 2013 Aggregate Knowledge, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.stat.hll.serialization;

/**
 * Reads 'words' of a fixed width, in sequence, from a byte array.
 *
 * @author timon
 */
public interface IWordDeserializer {
    /**
     * @return the next word in the sequence. Should not be called more than
     * {@link #totalWordCount()} times.
     */
    long readWord();

    /**
     * Returns the number of words that could be encoded in the sequence.<p/>
     *
     * NOTE:  the sequence that was encoded may be shorter than the value this
     *        method returns due to padding issues within bytes. This guarantees
     *        only an upper bound on the number of times {@link #readWord()}
     *        can be called.
     *
     * @return the maximum number of words that could be read from the sequence.
     */
    int totalWordCount();
}
