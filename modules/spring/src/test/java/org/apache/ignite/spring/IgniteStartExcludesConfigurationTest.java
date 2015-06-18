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

package org.apache.ignite.spring;

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.spring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.junits.common.*;

import java.net.*;
import java.util.*;

import static org.apache.ignite.internal.IgniteComponentType.*;

/**
 * Checks exclude properties in spring, exclude beans with not existing classes.
 */
public class IgniteStartExcludesConfigurationTest extends GridCommonAbstractTest {
    /** Tests spring exclude properties. */
    public void testExcludes() throws Exception {
        URL cfgLocation = U.resolveIgniteUrl(
            "modules/spring/src/test/java/org/apache/ignite/spring/sprint-exclude.xml");

        IgniteSpringHelper spring = SPRING.create(false);

        Collection<IgniteConfiguration> cfgs = spring.loadConfigurations(cfgLocation, "typeMetadata").get1();

        assert cfgs != null && cfgs.size() == 1;

        IgniteConfiguration cfg = cfgs.iterator().next();

        assert cfg.getCacheConfiguration().length == 1;

        assert cfg.getCacheConfiguration()[0].getTypeMetadata() == null;

        cfgs = spring.loadConfigurations(cfgLocation, "keyType").get1();

        assert cfgs != null && cfgs.size() == 1;

        cfg = cfgs.iterator().next();

        assert cfg.getCacheConfiguration().length == 1;

        Collection<CacheTypeMetadata> typeMetadatas = cfg.getCacheConfiguration()[0].getTypeMetadata();

        assert typeMetadatas.size() == 1;

        assert typeMetadatas.iterator().next().getKeyType() == null;

        cfgs = spring.loadConfigurations(cfgLocation).get1();

        assert cfgs != null && cfgs.size() == 1;

        cfg = cfgs.iterator().next();

        assert cfg.getCacheConfiguration().length == 1;

        typeMetadatas = cfg.getCacheConfiguration()[0].getTypeMetadata();

        assert typeMetadatas.size() == 1;

        assert "java.lang.Integer".equals(typeMetadatas.iterator().next().getKeyType());
    }
}
