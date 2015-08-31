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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.springframework.context.ApplicationContext;

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
    @SpringApplicationContextResource
    private ApplicationContext springCtx;

    /** {@inheritDoc} */
    @Override public void onLifecycleEvent(LifecycleEventType evt) {
        try {
            switch (evt) {
                case BEFORE_NODE_START:
                    break;

                case AFTER_NODE_START:
                    ignite.atomicSequence("ID", 0, true);

                    break;

                case BEFORE_NODE_STOP:
                    break;

                case AFTER_NODE_STOP:
                    break;
            }
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }
}