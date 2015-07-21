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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.jetbrains.annotations.*;

/**
 *
 */
public interface CacheIndexedObject extends CacheObject {
    /**
     * Gets portable object type ID.
     *
     * @return Type ID.
     */
    public int typeId();

    /**
     * Gets fully deserialized instance of portable object.
     *
     * @return Fully deserialized instance of portable object.
     */
    @Nullable public <T> T deserialize() throws IgniteException;

    /**
     * Gets field value.
     *
     * @param fieldName Field name.
     * @return Field value.
     */
    @Nullable public <T> T field(String fieldName);

    /**
     * Checks whether field is set.
     *
     * @param fieldName Field name.
     * @return {@code true} if field is set.
     */
    public boolean hasField(String fieldName);
}
