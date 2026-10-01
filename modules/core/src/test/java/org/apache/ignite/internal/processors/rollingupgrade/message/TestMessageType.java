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

package org.apache.ignite.internal.processors.rollingupgrade.message;

import java.util.Arrays;
import java.util.function.Predicate;
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest.TestVersions;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** */
public enum TestMessageType {
    /** */
    CORE_MSG("TestCoreMessage"),

    /** */
    PLUGIN_MSG("TestPluginMessage"),

    /** */
    DEFAULT_REGISTRY_MSG("TestDefaultRegistryMessage"),

    /** */
    CONTAINER_MSG("TestContainerMessage");

    /** */
    private final String clsName;

    /** */
    TestMessageType(String clsName) {
        this.clsName = clsName;
    }

    /** */
    @Nullable private Class<? extends Message> resolveClass(String cmpVers) {
        TestVersions vers = TestVersions.parse(cmpVers);

        if (this == PLUGIN_MSG && !vers.containsPlugin())
            return null;

        String cmpVer = this == PLUGIN_MSG ? vers.pluginVersion() : vers.coreVersion();

        String release = '_' + cmpVer.replace('.', '_');

        try {
            return Class.forName(TestMessageType.class.getPackageName() + '.' + clsName + release).asSubclass(Message.class);
        }
        catch (ClassNotFoundException ignored) {
            return null;
        }
    }

    /** */
    public static Class<? extends Message>[] resolveTestMessageClasses(String cmpVers) {
        return Arrays.stream(values()).map(msgType -> msgType.resolveClass(cmpVers)).toArray(Class[]::new);
    }

    /** */
    public TestDiscoveryMessage build(String cmpVers, Predicate<IgniteFeature> featureStatusProvider) throws Exception {
        return ((TestDiscoveryMessage)resolveClass(cmpVers).getConstructor().newInstance()).fill(featureStatusProvider);
    }
}
