/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.request;

/**
 *
 */
public class DataStructuresRequest extends GridRestRequest {
    /** Value to add/subtract. */
    private Long delta;

    /** Initial value for increment and decrement commands. */
    private Long init;

    /** Key. */
    private Object key;

    /**
     * @return Key.
     */
    public Object key() {
        return key;
    }

    /**
     * @param key Key.
     */
    public void key(Object key) {
        this.key = key;
    }

    /**
     * @return Delta for increment and decrement commands.
     */
    public Long delta() {
        return delta;
    }

    /**
     * @param delta Delta for increment and decrement commands.
     */
    public void delta(Long delta) {
        this.delta = delta;
    }

    /**
     * @return Initial value for increment and decrement commands.
     */
    public Long initial() {
        return init;
    }

    /**
     * @param init Initial value for increment and decrement commands.
     */
    public void initial(Long init) {
        this.init = init;
    }
}