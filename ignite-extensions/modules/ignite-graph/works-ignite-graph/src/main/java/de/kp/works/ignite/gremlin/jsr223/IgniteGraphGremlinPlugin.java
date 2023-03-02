package de.kp.works.ignite.gremlin.jsr223;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.ignite.gremlin.*;
import de.kp.works.ignite.gremlin.sql.IgniteGraphTraversalSource;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

public class IgniteGraphGremlinPlugin extends AbstractGremlinPlugin {

    private static final String NAME = "de.kp.works.ignite.gremlin";

    private static final ImportCustomizer imports = DefaultImportCustomizer.build()
            .addClassImports(
                    IgniteEdge.class,
                    IgniteElement.class,
                    IgniteGraph.class,
                    IgniteGraphConfiguration.class,
                    IgniteGraphFeatures.class,
                    IgniteProperty.class,
                    IgniteVertex.class,
                    IgniteVertexProperty.class,
                    IgniteGraphTraversalSource.class)
                    .create();

    private static final IgniteGraphGremlinPlugin instance = new IgniteGraphGremlinPlugin();

    public IgniteGraphGremlinPlugin() {
        super(NAME, imports);
    }

    public static IgniteGraphGremlinPlugin instance() {
        return instance;
    }
}
