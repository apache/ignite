/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.spring;

import java.io.FileInputStream;
import java.net.URL;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Checks starts from Stream.
 */
public class IgniteStartFromStreamConfigurationTest extends GridCommonAbstractTest {
    /** Tests starts from Stream. */
    public void testStartFromStream() throws Exception {
        String cfg = "examples/config/example-cache.xml";

        URL cfgLocation = U.resolveIgniteUrl(cfg);

        Ignite grid = Ignition.start(new FileInputStream(cfgLocation.getFile()));

        grid.cache(null).put("1", "1");

        assert grid.cache(null).get("1").equals("1");

        IgniteConfiguration icfg = Ignition.loadSpringBean(new FileInputStream(cfgLocation.getFile()), "ignite.cfg");

        assert icfg.getCacheConfiguration()[0].getAtomicityMode() == CacheAtomicityMode.ATOMIC;
    }

}