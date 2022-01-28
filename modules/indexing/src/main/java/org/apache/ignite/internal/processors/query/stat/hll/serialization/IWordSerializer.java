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
 * Writes 'words' of fixed width, in sequence, to a byte array.
 *
 * @author timon
 */
public interface IWordSerializer {
    /**
     * Writes the word to the backing array.
     *
     * @param  word the word to write.
     */
    void writeWord(final long word);

    /**
     * Returns the backing array of <code>byte</code>s that contain the serialized
     * words.
     * @return the serialized words as a <code>byte[]</code>.
     */
    byte[] getBytes();
}
