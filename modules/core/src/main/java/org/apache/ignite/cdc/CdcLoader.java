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

package org.apache.ignite.cdc;

import java.net.URL;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteExperimental;

import static org.apache.ignite.internal.IgniteComponentType.SPRING;

/**
 * Utility class to load {@link CdcMain} from Spring XML configuration.
 */
@IgniteExperimental
public class CdcLoader {
    /**
     * Loads {@link CdcMain} from XML configuration file.
     *
     * @param springXmlPath Path to XML configuration file.
     * @return {@code ChangeDataCapture} instance.
     * @throws IgniteCheckedException If failed.
     */
    public static CdcMain loadCdc(String springXmlPath) throws IgniteCheckedException {
        URL cfgUrl = U.resolveSpringUrl(springXmlPath);

        IgniteSpringHelper spring = SPRING.create(false);

        IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgTuple =
            spring.loadConfigurations(cfgUrl);

        if (cfgTuple.get1().size() > 1) {
            throw new IgniteCheckedException(
                "Exact 1 IgniteConfiguration should be defined. Found " + cfgTuple.get1().size()
            );
        }

        IgniteBiTuple<Collection<CdcConfiguration>, ? extends GridSpringResourceContext> cdcCfgs =
            spring.loadConfigurations(cfgUrl, CdcConfiguration.class);

        if (cdcCfgs.get1().size() > 1) {
            throw new IgniteCheckedException(
                "Exact 1 CaptureDataChangeConfiguration configuration should be defined. " +
                    "Found " + cdcCfgs.get1().size()
            );
        }

        return new CdcMain(
            cfgTuple.get1().iterator().next(),
            cfgTuple.get2(),
            cdcCfgs.get1().iterator().next()
        );
    }
}
