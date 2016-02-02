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

package org.apache.ignite.testframework.config.generator;

import java.util.Iterator;

/**
 * TODO: Add class description.
 */
public class StateIterator implements Iterator<State> {
    private ConfigurationParamethers params;

    public <T> StateIterator(ConfigurationParamethers params) {
        this.params = params;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return false; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public State next() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException();
    }
}
