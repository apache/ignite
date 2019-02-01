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

package org.apache.ignite.compatibility.framework;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.compatibility.IgniteCompatibilityRemoteNodeStartApp;
import org.apache.ignite.compatibility.framework.node.IgniteCompatibilityRemoteNode;
import org.apache.ignite.compatibility.start.IgniteCompatibilityStartNodeClosure;
import org.apache.ignite.internal.IgniteProperties;
import org.apache.ignite.internal.util.F0;
import org.junit.rules.TestRule;

/**
 * Represents the set of customizable parameters for compatibility framework that can be
 * overrided in other projects before all tests initialization.
 */
public class IgniteCompatibilityTestConfig {
    /** Instance. */
    private static IgniteCompatibilityTestConfig INSTANCE = new IgniteCompatibilityTestConfig();

    /** Current version. */
    private String currentVersion = IgniteProperties.get("ignite.version");

    /** Shutdown hook. */
    private Thread shutdownHook = new Thread(IgniteCompatibilityRemoteNode::stopTest);

    /** Run rule factory. */
    private Function<Long, TestRule> runRuleFactory = IgniteCompatibilityTestExecutionRule::new;

    /** Start node closure factory. */
    private Supplier<IgniteCompatibilityStartNodeClosure> startNodeClosureFactory = IgniteCompatibilityStartNodeClosure::new;

    /** Default base path. */
    private String defaultBasePath = "modules/compatibility/ignite-versions/";

    /** Test apps. */
    private List<String> testApps = Arrays.asList(
        IgniteCompatibilityRemoteNodeStartApp.class.getName(),
        "org.apache.ignite.compatibility.framework.node.IgniteCompatibilityRemoteNode"
    );

    /** Versions directory. */
    private String versionsDirectory = System.getProperty("ignVersionsDirectory");

    /** Assertion excludes. */
    private Set<String> assertionExcludes = Collections.emptySet();

    /**
     * @return Config instance.
     */
    public static IgniteCompatibilityTestConfig get() {
        return INSTANCE;
    }

    /**
     * @return current version of Ignite.
     */
    public String currentVersion() {
        return currentVersion;
    }

    /**
     * @param currentVersion Current version.
     */
    public void setCurrentVersion(String currentVersion) {
        this.currentVersion = currentVersion;
    }

    /**
     * @return Shutdown hook for framework app.
     */
    public Thread shutdownHook() {
        return shutdownHook;
    }

    /**
     * @param shutdownHook Shutdown hook.
     */
    public void setShutdownHook(Thread shutdownHook) {
        this.shutdownHook = shutdownHook;
    }

    /**
     * @param timeout Timeout.
     * @return Tests run rule.
     */
    public TestRule runRule(long timeout) {
        return runRuleFactory.apply(timeout);
    }

    /**
     * @param runRuleFactory Run rule factory.
     */
    public void setRunRuleFactory(Function<Long, TestRule> runRuleFactory) {
        this.runRuleFactory = runRuleFactory;
    }

    /**
     * @return Start node closure.
     */
    public Supplier<IgniteCompatibilityStartNodeClosure> startNodeClosure() {
        return startNodeClosureFactory;
    }

    /**
     * @param startNodeClosureFactory Start node closure factory.
     */
    public void setStartNodeClosureFactory(Supplier<IgniteCompatibilityStartNodeClosure> startNodeClosureFactory) {
        this.startNodeClosureFactory = startNodeClosureFactory;
    }

    /**
     * @return Default base path for tests.
     */
    public String defaultBasePath() {
        return defaultBasePath;
    }

    /**
     * @param defaultBasePath Default base path.
     */
    public void setDefaultBasePath(String defaultBasePath) {
        this.defaultBasePath = defaultBasePath;
    }

    /**
     * @return Test apps class names.
     */
    public List<String> testApps() {
        return testApps;
    }

    /**
     * @param testApps Test apps.
     */
    public void setTestApps(List<String> testApps) {
        this.testApps = testApps;
    }

    /**
     * @return Versions directory.
     */
    public String versionsDirectory() {
        return versionsDirectory;
    }

    /**
     * @param versionsDir Versions directory.
     */
    public void setVersionsDirectory(String versionsDir) {
        this.versionsDirectory = versionsDir;
    }

    /**
     * @return Assertion excludes.
     */
    public Set<String> assertionExcludes() {
        return assertionExcludes;
    }

    /**
     * @param assertionExcludes Assertion excludes.
     */
    public void setAssertionExcludes(Set<String> assertionExcludes) {
        this.assertionExcludes = assertionExcludes;
    }
}
