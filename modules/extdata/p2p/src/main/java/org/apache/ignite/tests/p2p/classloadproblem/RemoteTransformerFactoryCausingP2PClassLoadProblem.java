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

package org.apache.ignite.tests.p2p.classloadproblem;

import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Remote transformer factory implementation that breaks p2p class-loading and causes another class to be loaded
 * using p2p when its transformation method is called.
 * <p>
 * Used to test the situation when p2p class-loading fails due to a peer failing or leaving.
 */
public class RemoteTransformerFactoryCausingP2PClassLoadProblem
    implements Factory<IgniteClosure<CacheEntryEvent<Integer, String>, String>> {
    /***/
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public IgniteClosure<CacheEntryEvent<Integer, String>, String> create() {
        return new Transformer();
    }

    /***/
    private class Transformer implements IgniteClosure<CacheEntryEvent<Integer, String>, String> {
        /** {@inheritDoc} */
        @Override public String apply(CacheEntryEvent<Integer, String> event) {
            P2PClassLoadingProblems.triggerP2PClassLoadingProblem(getClass(), ignite);

            return event.getValue();
        }
    }
}
