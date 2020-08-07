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

package org.apache.ignite.internal.ducktest.utils;

import java.util.Base64;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 *
 */
public class IgniteApplicationService {
    /** Logger. */
    private static final Logger log = LogManager.getLogger(IgniteApplicationService.class.getName());

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws Exception {
        log.info("Starting Application... [params=" + args[0] + "]");

        String[] params = args[0].split(",");

        Class<?> clazz = Class.forName(params[0]);

        IgniteBiTuple<IgniteConfiguration, GridSpringResourceContext> cfgs = IgnitionEx.loadConfiguration(params[1]);

        IgniteConfiguration cfg = cfgs.get1();

        assert cfg.isClientMode();

        log.info("Starting Ignite node...");

        try (Ignite ignite = Ignition.start(cfg)) {
            IgniteAwareApplication app = (IgniteAwareApplication)clazz.getConstructor(Ignite.class).newInstance(ignite);

            ObjectMapper mapper = new ObjectMapper();

            JsonNode jsonNode = params.length > 2 ?
                mapper.readTree(Base64.getDecoder().decode(params[2])) : mapper.createObjectNode();

            app.start(jsonNode);
        }
    }
}
