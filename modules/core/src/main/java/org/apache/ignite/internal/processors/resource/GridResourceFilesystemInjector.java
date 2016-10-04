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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.FilesystemResource;

/**
 * Grid service injector.
 */
public class GridResourceFilesystemInjector extends GridResourceBasicInjector<IgniteFileSystem> {
    /** */
    private Ignite ignite;
    /** */
    private IgniteFileSystem igfs;

    /**
     * @param ignite Grid.
     * @param igfs Filesystem to inject.
     */
    public GridResourceFilesystemInjector(Ignite ignite, IgniteFileSystem igfs) {
        super(null);

        this.ignite = ignite;
        this.igfs = igfs;
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceField field, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        FilesystemResource ann = (FilesystemResource)field.getAnnotation();

        if (!"".equals(ann.name()) && igfs == null)
            igfs = F.first(ignite.fileSystems());

        if (igfs != null)
            GridResourceUtils.inject(field.getField(), target, igfs);
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        FilesystemResource ann = (FilesystemResource)mtd.getAnnotation();

        if (!"".equals(ann.name()) && igfs == null)
            igfs = F.first(ignite.fileSystems());

        if (igfs == null)
            return;

        Class<?>[] types = mtd.getMethod().getParameterTypes();

        if (types.length != 1)
            throw new IgniteCheckedException("Setter does not have single parameter of required type [type=" +
                igfs.getClass().getName() + ", setter=" + mtd + ']');

        if (igfs != null)
            GridResourceUtils.inject(mtd.getMethod(), target, igfs);
    }
}