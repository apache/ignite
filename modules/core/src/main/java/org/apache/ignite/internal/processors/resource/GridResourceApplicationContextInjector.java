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

package org.apache.ignite.internal.processors.resource;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.ApplicationContext;
import org.apache.ignite.internal.cache.ApplicationContextInternal;
import org.apache.ignite.internal.managers.deployment.GridDeployment;

/**
 * {@link ApplicationContext} injector.
 */
public class GridResourceApplicationContextInjector extends GridResourceBasicInjector<Collection<ApplicationContext>> {
    /** */
    public GridResourceApplicationContextInjector() {
        super(null);
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceField field, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        ApplicationContext ctx = ApplicationContextInternal.applicationContext();

        if (ctx != null)
            GridResourceUtils.inject(field.getField(), target, ctx);
    }
}
