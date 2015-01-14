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

package org.gridgain.grid.kernal;

import org.apache.ignite.mbean.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Management bean that provides access to {@link org.apache.ignite.Ignition}.
 */
public class IgnitionMBeanAdapter implements IgnitionMBean {
    /** {@inheritDoc} */
    @Override public String getState() {
        return G.state().toString();
    }

    /** {@inheritDoc} */
    @Override public String getState(String name) {
        if (F.isEmpty(name))
            name = null;

        return G.state(name).toString();
    }

    /** {@inheritDoc} */
    @Override public boolean stop(boolean cancel) {
        return G.stop(cancel);
    }

    /** {@inheritDoc} */
    @Override public boolean stop(String name, boolean cancel) {
        return G.stop(name, cancel);
    }

    /** {@inheritDoc} */
    @Override public void stopAll(boolean cancel) {
        G.stopAll(cancel);
    }

    /** {@inheritDoc} */
    @Override public void restart(boolean cancel) {
        G.restart(cancel);
    }
}
