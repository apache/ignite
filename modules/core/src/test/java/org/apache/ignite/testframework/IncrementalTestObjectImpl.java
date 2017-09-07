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

package org.apache.ignite.testframework;

/**
 * Example implementation of {@link IncrementalTestObject}
 */
public class IncrementalTestObjectImpl implements IncrementalTestObject {
    /** Id. */
    private int id;

    /** Name. */
    private String name;

    /**
     * @param id Id.
     */
    public IncrementalTestObjectImpl(int id) {
        this.id = id;
        this.name = "KeyObject" + id;
    }

    /** {@inheritDoc} */
    @Override public IncrementalTestObject increment(int times) {
        return new IncrementalTestObjectImpl(id + times);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "KeyObject{" +
            "id=" + id +
            ", name='" + name + '\'' +
            '}';
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        IncrementalTestObjectImpl obj = (IncrementalTestObjectImpl)o;

        return id == obj.id && name.equals(obj.name);

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id;
    }
}
