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

package org.apache.ignite.binary;

/**
 * Interface that allows to implement custom serialization
 * logic for binary objects. IgniteObject is not required
 * to implement this interface, in which case Ignite will automatically
 * serialize binary objects using reflection.
 * <p>
 * This interface, in a way, is analogous to {@link java.io.Externalizable}
 * interface, which allows users to override default serialization logic,
 * usually for performance reasons. The only difference here is that binary
 * serialization is already very fast and implementing custom serialization
 * logic for binary does not provide significant performance gains.
 */
public interface Binarylizable {
    /**
     * Writes fields to provided writer.
     *
     * @param writer Binary object writer.
     * @throws BinaryObjectException In case of error.
     */
    public void writeBinary(BinaryWriter writer) throws BinaryObjectException;

    /**
     * Reads fields from provided reader.
     *
     * @param reader Binary object reader.
     * @throws BinaryObjectException In case of error.
     */
    public void readBinary(BinaryReader reader) throws BinaryObjectException;
}
