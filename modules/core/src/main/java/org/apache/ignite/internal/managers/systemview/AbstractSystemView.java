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

import java.util.Iterator;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.plugin.security.SecurityPermission.SYSTEM_VIEW_READ;

/** Abstract system view. */
abstract class AbstractSystemView<R> implements SystemView<R> {
    /** Name of the view. */
    private final String name;

    /** Description of the view. */
    private final String desc;

    /** {@link IgniteSecurity} for data access authorization. */
    private final IgniteSecurity security;

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
    AbstractSystemView(String name, String desc, SystemViewRowAttributeWalker<R> walker, IgniteSecurity security) {
        A.notNull(walker, "walker");
        A.notNull(security, "security");

        this.name = name;
        this.desc = desc;
        this.walker = walker;
        this.security = security;
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

    /** {@inheritDoc} */
    @NotNull @Override public final Iterator<R> iterator() {
        authorize();

        return iteratorNoAuth();
    }

    /**
     * {@link Iterable#iterator()} implementation without authorization.
     */
    @NotNull protected abstract Iterator<R> iteratorNoAuth();

    /** {@inheritDoc} */
    @Override public final int size() {
        authorize();

        return sizeNoAuth();
    }

    /**
     * {@link SystemView#size()} implementation without authorization.
     */
    protected abstract int sizeNoAuth();

    /**
     * Authorizes {@link SecurityPermission#SYSTEM_VIEW_READ} permission.
     */
    protected final void authorize() {
        security.authorize(SYSTEM_VIEW_READ);
    }
}
