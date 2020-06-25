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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ConfigurationBuilder;

/**
 * See {@link RelMetadataQuery}
 */
public class RelMetadataQueryEx extends RelMetadataQuery {
    static {
        ConfigurationBuilder cfg = new ConfigurationBuilder()
            .forPackages("org.apache.ignite.internal.processors.query.calcite.rel")
            .addClassLoaders(U.gridClassLoader())
            .addScanners(new SubTypesScanner());

        List<Class<? extends RelNode>> types = new Reflections(cfg)
            .getSubTypesOf(IgniteRel.class).stream()
            .filter(type -> !type.isInterface())
            .filter(type -> !Modifier.isAbstract(type.getModifiers()))
            .collect(Collectors.toList());

        JaninoRelMetadataProvider.DEFAULT.register(types);
    }

    /** */
    private static final IgniteMetadata.NodesMappingMetadata.Handler SOURCE_DISTRIBUTION_INITIAL_HANDLER =
        initialHandler(IgniteMetadata.NodesMappingMetadata.Handler.class);

    /** */
    private IgniteMetadata.NodesMappingMetadata.Handler sourceDistributionHandler;

    /**
     * Factory method.
     *
     * @return return Metadata query instance.
     */
    public static RelMetadataQueryEx create() {
        return create(IgniteMetadata.METADATA_PROVIDER);
    }

    /**
     * Factory method.
     *
     * @return return Metadata query instance.
     */
    public static RelMetadataQueryEx create(RelMetadataProvider metadataProvider) {
        THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(metadataProvider));
        try {
            return new RelMetadataQueryEx();
        }
        finally {
            THREAD_PROVIDERS.remove();
        }
    }

    /** */
    private RelMetadataQueryEx() {
        sourceDistributionHandler = SOURCE_DISTRIBUTION_INITIAL_HANDLER;
    }

    /**
     * Calculates data location mapping for a query fragment the given relation node is a root of.
     *
     * @param rel Relational node.
     * @return Fragment meta information.
     */
    public NodesMapping nodesMapping(RelNode rel) {
        for (;;) {
            try {
                return sourceDistributionHandler.nodesMapping(rel, this);
            } catch (JaninoRelMetadataProvider.NoHandler e) {
                sourceDistributionHandler = revise(e.relClass, IgniteMetadata.NodesMappingMetadata.DEF);
            }
        }
    }
}
