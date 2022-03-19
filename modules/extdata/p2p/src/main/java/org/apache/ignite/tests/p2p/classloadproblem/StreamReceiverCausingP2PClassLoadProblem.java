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

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.stream.StreamReceiver;

/**
 * {@link StreamReceiver} implementation that breaks p2p class-loading and causes another class to be loaded
 * using p2p when its receive() method is called.
 * <p>
 * Used to test the situation when p2p class-loading fails due to a peer failing or leaving.
 * <p>
 * NB: The receive() method should only be called on an instance of this class loaded by a P2P class-loader.
 */
public class StreamReceiverCausingP2PClassLoadProblem implements StreamReceiver<Object, Object> {
    /***/
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public void receive(IgniteCache<Object, Object> cache, Collection<Map.Entry<Object, Object>> entries)
        throws IgniteException {
        P2PClassLoadingProblems.triggerP2PClassLoadingProblem(getClass(), ignite);
    }
}
