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
 * TODO update javadoc.
 *
 * Internal ID mapper. Mimics ID mapper interface, but provides default implementation and offers slightly better
 * performance on micro-level in default case because it doesn't need virtual calls.
 */
public class BinaryFullNameIdMapper implements BinaryIdMapper {
    /** {@inheritDoc} */
    @Override public int typeId(String clsName) {
        int h = clsName.hashCode();

        if (h != 0)
            return h;
        else
            throw new BinaryObjectException("Default binary ID mapper resolved type ID to zero " +
                "(either change type's name or use custom ID mapper) [name=" + clsName + ']');
    }

    /** {@inheritDoc} */
    @Override public int fieldId(int typeId, String fieldName) {
        int h = fieldName.hashCode();

        if (h != 0)
            return h;
        else
            throw new BinaryObjectException("Default binary ID mapper resolved field ID to zero " +
                "(either change filed's name or use custom ID mapper) [name=" + fieldName + ']');
    }
}
