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

package org.apache.ignite.internal.network.serialization.marshal;

import org.apache.ignite.internal.network.serialization.Null;

/**
 * Utilities to work with object class.
 */
class ObjectClass {
    /**
     * Determines object class for the needs of marshalling. If the object is null, we return {@link Null}, otherwise
     * the proper object class.
     *
     * @param object object at which to look
     * @return {@link Null} if the object is {@code null}, otherwise the object's class
     */
    static Class<?> objectClass(Object object) {
        return object != null ? object.getClass() : Null.class;
    }

    private ObjectClass() {
    }
}
