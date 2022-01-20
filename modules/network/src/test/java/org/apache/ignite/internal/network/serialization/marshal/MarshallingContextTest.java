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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Objects;
import org.junit.jupiter.api.Test;

class MarshallingContextTest {
    private final MarshallingContext context = new MarshallingContext();

    @Test
    void firstMemorizationReturnsFreshObjectId() {
        long flaggedObjectId = context.memorizeObject(new Object(), false);

        assertFalse(FlaggedObjectIds.isAlreadySeen(flaggedObjectId));
    }

    @Test
    void secondMemorizationOfSameObjectReturnsAlreadySeenObjectId() {
        Object object = new Object();

        context.memorizeObject(object, false);

        long flaggedObjectId = context.memorizeObject(object, false);

        assertTrue(FlaggedObjectIds.isAlreadySeen(flaggedObjectId));
    }

    @Test
    void differentInstancesAreNotSameForMemorizationEvenWhenEqual() {
        Key key1 = new Key("test");
        Key key2 = new Key("test");

        context.memorizeObject(key1, false);

        long flaggedObjectId = context.memorizeObject(key2, false);

        assertFalse(FlaggedObjectIds.isAlreadySeen(flaggedObjectId));
    }

    @Test
    void memorizationThrowsOnNullObject() {
        assertThrows(NullPointerException.class, () -> context.memorizeObject(null, false));
    }

    @Test
    void memorizationGeneratesNewObjectIdForAlreadySeenObjectWhenUnsharedIsTrue() {
        var object = new Object();
        context.memorizeObject(object, false);

        long flaggedObjectId = context.memorizeObject(object, true);

        assertFalse(FlaggedObjectIds.isAlreadySeen(flaggedObjectId));
    }

    @Test
    void unsharedObjectDoesNotGetRememberedAsSeen() {
        var object = new Object();
        context.memorizeObject(object, true);

        long flaggedObjectId = context.memorizeObject(object, false);

        assertFalse(FlaggedObjectIds.isAlreadySeen(flaggedObjectId));
    }

    private static class Key {
        private final String key;

        private Key(String key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key1 = (Key) o;
            return Objects.equals(key, key1.key);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }
}
