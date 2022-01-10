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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Objects;
import org.junit.jupiter.api.Test;

class MarshallingContextTest {
    private final MarshallingContext context = new MarshallingContext();

    @Test
    void firstMemorizationReturnsFalse() {
        assertNull(context.rememberAsSeen(new Object()));
    }

    @Test
    void secondMemorizationOfSameObjectReturnsTrue() {
        Object object = new Object();

        context.rememberAsSeen(object);

        assertNotNull(context.rememberAsSeen(object));
    }

    @Test
    void differentInstancesAreNotSameForMemorizationEvenWhenEqual() {
        Key key1 = new Key("test");
        Key key2 = new Key("test");

        context.rememberAsSeen(key1);

        assertNull(context.rememberAsSeen(key2));
    }

    @Test
    void ignoresNulls() {
        context.rememberAsSeen(null);

        assertNull(context.rememberAsSeen(null));
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
