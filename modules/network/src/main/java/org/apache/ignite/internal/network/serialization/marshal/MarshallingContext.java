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

import static java.util.Collections.unmodifiableSet;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Context using during marshalling of an object graph accessible from a root object.
 */
class MarshallingContext {
    private final Set<ClassDescriptor> usedDescriptors = new HashSet<>();

    private final Map<Object, Integer> objectsToRefIds = new IdentityHashMap<>();

    private int nextRefId = 0;

    public void addUsedDescriptor(ClassDescriptor descriptor) {
        usedDescriptors.add(descriptor);
    }

    public Set<ClassDescriptor> usedDescriptors() {
        return unmodifiableSet(usedDescriptors);
    }

    /**
     * If the object was already seen before, its ID is returned; otherwise, it's memorized as seen with a fresh ID.
     *
     * @param object object to operate upon
     * @return object ID if it was seen earlier or {@code null} if the object is new
     */
    @Nullable
    public Integer rememberAsSeen(@Nullable Object object) {
        if (object == null) {
            return null;
        }

        Integer prevRefId = objectsToRefIds.get(object);
        if (prevRefId != null) {
            return prevRefId;
        } else {
            int newRefId = nextRefId();

            objectsToRefIds.put(object, newRefId);

            return null;
        }
    }

    private int nextRefId() {
        return nextRefId++;
    }

    /**
     * Returns a reference ID by the given object.
     *
     * @param object lookup object
     * @return object ID
     */
    public int referenceId(Object object) {
        Integer refId = objectsToRefIds.get(object);

        if (refId == null) {
            throw new IllegalStateException("No reference created yet for " + object);
        }

        return refId;
    }
}
