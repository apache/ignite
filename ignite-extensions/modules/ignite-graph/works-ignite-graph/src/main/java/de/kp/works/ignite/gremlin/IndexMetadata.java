package de.kp.works.ignite.gremlin;

import de.kp.works.ignite.graph.ElementType;

/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

public class IndexMetadata {

    private final Key key;
    private final boolean isUnique;
    private State state;
    protected final Long createdAt;
    protected Long updatedAt;

    public IndexMetadata(ElementType type, String label, String propertyKey,
                         boolean isUnique, State state, Long createdAt, Long updatedAt) {
        this.key = new Key(type, label, propertyKey);
        this.isUnique = isUnique;
        this.state = state;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public Key key() {
        return key;
    }

    public ElementType type() {
        return key.type();
    }

    public String label() {
        return key.label();
    }

    public String propertyKey() {
        return key.propertyKey();
    }

    public boolean isUnique() {
        return isUnique;
    }

    public State state() {
        return state;
    }

    public void state(State state) {
        this.state = state;
    }

    public Long createdAt() {
        return createdAt;
    }

    public Long updatedAt() {
        return updatedAt;
    }

    public void updatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public String toString() {
        return key.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexMetadata that = (IndexMetadata) o;

        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    public static class Key {
        private final ElementType type;
        private final String label;
        private final String propertyKey;

        public Key(ElementType type, String label, String propertyKey) {
            this.type = type;
            this.label = label;
            this.propertyKey = propertyKey;
        }

        public ElementType type() {
            return type;
        }

        public String label() {
            return label;
        }

        public String propertyKey() {
            return propertyKey;
        }

        @Override
        public String toString() {
            return type + " INDEX " + " " + label + "(" + propertyKey() + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key = (Key) o;

            if (type != key.type) return false;
            if (!label.equals(key.label)) return false;
            return propertyKey.equals(key.propertyKey);
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + label.hashCode();
            result = 31 * result + propertyKey.hashCode();
            return result;
        }
    }

    public enum State {
        CREATED,
        BUILDING,
        ACTIVE,
        INACTIVE, // an intermediate state to wait for clients to stop using an index
        DROPPED
    }
}
