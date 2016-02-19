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
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 *
 */
public abstract class AbstractConfigurationGenerator<T> implements ConfigurationGenerator<T> {
    /** */
    private final ConfigurationParamethers<T> params;

    /** */
    private final PermutationsIterator stateIter;

    /** */
    private State curState;

    /**
     * @param params Paramethers.
     */
    public AbstractConfigurationGenerator(ConfigurationParamethers<T> params) {
        A.notNull(params, "params");

        this.params = params;
        stateIter = new PermutationsIterator(null);
    }

    /** {@inheritDoc} */
    @Override public T getConfiguration() {
        T cfg = createNew();

        for (int i = 0; i < params.paramethersCount(); i++) {
            ConfigurationParameter<T> param = params.get(i, curState.variableNumberAt(i));

            param.apply(cfg);
        }

        return cfg;
    }

    /**
     * @return Empty configuration.
     */
    protected abstract T createNew();

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        return new ConfigurationIterator();
    }

    /**
     *
     */
    private class ConfigurationIterator implements Iterator<T> {
        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return stateIter.hasNext();
        }

        /** {@inheritDoc} */
        @Override public T next() {
//            curState = stateIter.next(); // TODO

            return getConfiguration();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
