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
import org.apache.ignite.internal.cdc.ChangeDataCapture;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.IgniteComponentType.SPRING;

/**
 * Utility class to load {@link ChangeDataCapture} from Spring XML configuration.
 */
public class ChangeDataCaptureLoader {
    /**
     * Loads {@link ChangeDataCapture} from XML configuration file and possible error message.
     * If load fails then error message wouldn't be null.
     *
     * @param springXmlPath Path to XML configuration file.
     * @return Tuple of {@code ChangeDataCapture} and error message.
     * @throws IgniteCheckedException If failed.
     */
    public static ChangeDataCapture loadChangeDataCapture(
        String springXmlPath
    ) throws IgniteCheckedException {
        URL cfgUrl = U.resolveSpringUrl(springXmlPath);

        IgniteSpringHelper spring = SPRING.create(false);

        IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgTuple =
            spring.loadConfigurations(cfgUrl);

        if (cfgTuple.get1().size() > 1) {
            throw new IgniteCheckedException(
                "Exact 1 IgniteConfiguration should be defined. Found " + cfgTuple.get1().size()
            );
        }

        IgniteBiTuple<Collection<ChangeDataCaptureConfiguration>, ? extends GridSpringResourceContext> cdcCfgs =
            spring.loadConfigurations(cfgUrl, ChangeDataCaptureConfiguration.class);

        if (cdcCfgs.get1().size() > 1) {
            throw new IgniteCheckedException(
                "Exact 1 CaptureDataChangeConfiguration configuration should be defined. " +
                    "Found " + cdcCfgs.get1().size()
            );
        }

        return new ChangeDataCapture(
            cfgTuple.get1().iterator().next(),
            cfgTuple.get2(),
            cdcCfgs.get1().iterator().next()
        );
    }
}
