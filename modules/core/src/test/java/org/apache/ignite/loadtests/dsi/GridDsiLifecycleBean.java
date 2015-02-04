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

package org.apache.ignite.loadtests.dsi;

import org.apache.ignite.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.resources.*;
import org.springframework.context.*;

/**
 *
 */
public class GridDsiLifecycleBean implements LifecycleBean {
    /**
     * Ignite instance will be automatically injected. For additional resources
     * that can be injected into lifecycle beans see
     * {@link org.apache.ignite.lifecycle.LifecycleBean} documentation.
     */
    @IgniteInstanceResource
    private Ignite ignite;

    /** */
    @SuppressWarnings("UnusedDeclaration")
    @IgniteSpringApplicationContextResource
    private ApplicationContext springCtx;

    /** {@inheritDoc} */
    @Override public void onLifecycleEvent(LifecycleEventType evt) {
        try {
            switch (evt) {
                case BEFORE_GRID_START:
                    break;

                case AFTER_GRID_START:
                    ignite.cache("PARTITIONED_CACHE").dataStructures().atomicSequence("ID", 0, true);

                    break;

                case BEFORE_GRID_STOP:
                    break;

                case AFTER_GRID_STOP:
                    break;
            }
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }
}
