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

package org.apache.ignite.internal.client;

import org.apache.ignite.portables.*;

import java.util.*;

/**
 * Task where argument and result are {@link ClientTestPortable}.
 */
public class ClientPortableArgumentTask extends TaskSingleJobSplitAdapter {
    /** {@inheritDoc} */
    @Override protected Object executeJob(int gridSize, Object arg) {
        Collection args = (Collection)arg;

        Iterator<Object> it = args.iterator();

        assert args.size() == 2 : args.size();

        boolean expPortable = (Boolean)it.next();

        ClientTestPortable p;

        if (expPortable) {
            PortableObject obj = (PortableObject)it.next();

            p = obj.deserialize();
        }
        else
            p = (ClientTestPortable)it.next();

        assert p != null;

        return new ClientTestPortable(p.i + 1, true);
    }
}
