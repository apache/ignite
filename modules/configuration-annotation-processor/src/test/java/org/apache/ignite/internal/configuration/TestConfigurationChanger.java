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

package org.apache.ignite.internal.configuration;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.InnerNode;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.internalSchemaExtensions;

/** Implementation of {@link ConfigurationChanger} to be used in tests. Has no support of listeners. */
public class TestConfigurationChanger extends ConfigurationChanger {
    /** Runtime implementations generator for node classes. */
    private final ConfigurationAsmGenerator cgen;

    /**
     * Constructor.
     *
     * @param cgen Runtime implementations generator for node classes. Will be used to instantiate nodes objects.
     * @param rootKeys Configuration root keys.
     * @param validators Validators.
     * @param storage Configuration storage.
     * @param internalSchemaExtensions Internal extensions ({@link InternalConfiguration})
     *      of configuration schemas ({@link ConfigurationRoot} and {@link Config}).
     * @throws IllegalArgumentException If the configuration type of the root keys is not equal to the storage type.
     */
    public TestConfigurationChanger(
        ConfigurationAsmGenerator cgen,
        Collection<RootKey<?, ?>> rootKeys,
        Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators,
        ConfigurationStorage storage,
        Collection<Class<?>> internalSchemaExtensions
    ) {
        super(
            (oldRoot, newRoot, revision) -> completedFuture(null),
            rootKeys,
            validators,
            storage
        );

        this.cgen = cgen;

        Map<Class<?>, Set<Class<?>>> extensions = internalSchemaExtensions(internalSchemaExtensions);

        rootKeys.forEach(key -> cgen.compileRootSchema(key.schemaClass(), extensions));
    }

    /** {@inheritDoc} */
    @Override public InnerNode createRootNode(RootKey<?, ?> rootKey) {
        return cgen.instantiateNode(rootKey.schemaClass());
    }
}
