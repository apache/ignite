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

package org.apache.ignite.internal.managers.systemview;

import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/** Abstract system view. */
abstract class AbstractSystemView<R> implements SystemView<R> {
    /** Name of the view. */
    private final String name;

    /** Description of the view. */
    private final String desc;

    /**
     * Row attribute walker.
     *
     * @see "org.apache.ignite.codegen.MonitoringRowAttributeWalkerGenerator"
     */
    private final SystemViewRowAttributeWalker<R> walker;

    /**
     * @param name Name.
     * @param desc Description.
     * @param walker Walker.
     */
    AbstractSystemView(String name, String desc, SystemViewRowAttributeWalker<R> walker) {
        A.notNull(walker, "walker");

        this.name = name;
        this.desc = desc;
        this.walker = walker;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override public SystemViewRowAttributeWalker<R> walker() {
        return walker;
    }
}
