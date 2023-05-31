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

package org.apache.ignite.internal.visor.cache;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.cache.CacheCreateCommandArg;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.resources.IgniteInstanceResource;
import static org.apache.ignite.internal.IgniteComponentType.SPRING;

/**
 * Task to create caches from Spring XML configuration.
 */
@GridInternal
public class VisorCacheCreateTask extends VisorOneNodeTask<CacheCreateCommandArg, Set<String>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<CacheCreateCommandArg, Set<String>> job(CacheCreateCommandArg springXmlConfig) {
        return new VisorCacheCreateJob(springXmlConfig, false);
    }

    /** */
    private static class VisorCacheCreateJob extends VisorJob<CacheCreateCommandArg, Set<String>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        protected transient IgniteEx ignite;

        /** */
        protected VisorCacheCreateJob(CacheCreateCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Set<String> run(CacheCreateCommandArg arg) throws IgniteException {
            if (F.isEmpty(arg.fileContent()))
                throw new IllegalArgumentException("Configurations not specified.");

            IgniteSpringHelper spring;

            try {
                spring = SPRING.create(false);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to create caches. " + SPRING.module() + " module is not configured.", e);
            }

            Collection<CacheConfiguration> ccfgs;

            try {
                ccfgs = spring.loadConfigurations(new ByteArrayInputStream(arg.fileContent().getBytes()),
                    CacheConfiguration.class).get1();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to create caches. Make sure that Spring XML contains '" +
                    CacheConfiguration.class.getName() + "' beans.", e);
            }

            Collection<IgniteCache> caches = ignite.createCaches(ccfgs);

            return caches.stream().map(Cache::getName).collect(Collectors.toCollection(TreeSet::new));
        }
    }
}
