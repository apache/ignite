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

package org.apache.ignite.tests.p2p;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Simple event filter
 */
@SuppressWarnings({"ProhibitedExceptionThrown"})
public class GridP2PEventFilterExternalPath2 implements IgnitePredicate<Event> {
    /** Instance of grid. Used for save class loader and injected resource. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public boolean apply(Event evt) {
        try {
            int[] res = new int[] {
                System.identityHashCode(getClass().getClassLoader())
            };

            ignite.message(ignite.cluster().forRemotes()).send(null, res);
        }
        catch (IgniteException e) {
            throw new RuntimeException(e);
        }

        return true;
    }
}