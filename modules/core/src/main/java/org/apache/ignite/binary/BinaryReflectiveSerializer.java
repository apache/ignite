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
 * Binary serializer which writes object fields using reflection. Transient fields are not written.
 */
public class BinaryReflectiveSerializer implements BinarySerializer {
    /** {@inheritDoc} */
    @Override public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException {
        assert false : "Method should not be called directly.";
    }

    /** {@inheritDoc} */
    @Override public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException {
        assert false : "Method should not be called directly.";
    }
}
