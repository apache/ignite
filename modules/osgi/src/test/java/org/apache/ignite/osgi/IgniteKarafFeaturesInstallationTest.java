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

package org.apache.ignite.osgi;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;

import org.apache.karaf.features.Feature;
import org.apache.karaf.features.FeaturesService;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.karaf.options.LogLevelOption;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.maven;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.CoreOptions.systemProperty;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFileExtend;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.logLevel;

/**
 * Pax Exam test class to check if all features could be resolved and installed.
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class IgniteKarafFeaturesInstallationTest {

    /** Features we do not expect to be installed. */
    private static final Set<String> IGNORED_FEATURES = new HashSet<>(
        Arrays.asList("ignite-log4j", "ignite-scalar-2.10"));

    /** Regex matching ignite features. */
    private static final String IGNITE_FEATURES_NAME_REGEX = "ignite.*";

    /** Project version. */
    private static final String PROJECT_VERSION = System.getProperty("projectVersion");

    /** Pax Exam will inject the Bundle Context here. */
    @Inject
    private BundleContext bundleCtx;

    /** Pax Exam will inject the Karaf Features Service. */
    @Inject
    private FeaturesService featuresSvc;

    /**
     * Returns configuration for Karaf container.
     *
     * @return The configuration.
     */
    @Configuration
    public Option[] config() {
        return options(

            // Specify which version of Karaf to use.
            karafDistributionConfiguration()
                .frameworkUrl(maven().groupId("org.apache.karaf").artifactId("apache-karaf").type("tar.gz")
                    .versionAsInProject())
                .karafVersion(System.getProperty("karafVersion"))
                .useDeployFolder(false)
                .unpackDirectory(new File("target/paxexam/unpack")),

            // Add JUnit bundles.
            junitBundles(),

            // Add the additional JRE exports that Ignite requires.
            editConfigurationFileExtend("etc/jre.properties", "jre-1.7", "sun.nio.ch"),
            editConfigurationFileExtend("etc/jre.properties", "jre-1.8", "sun.nio.ch"),

            // Make log level INFO.
            logLevel(LogLevelOption.LogLevel.INFO),

            // Add our features repository.
            features(mavenBundle()
                        .groupId("org.apache.ignite").artifactId("ignite-osgi-karaf")
                        .version(System.getProperty("projectVersion")).type("xml/features"),
                "ignite-all"),

            // Propagate the projectVersion system property.
            systemProperty("projectVersion").value(System.getProperty("projectVersion"))
        );
    }

    /**
     * @throws Exception
     */
    @Test
    public void testAllBundlesActiveAndFeaturesInstalled() throws Exception {
        // Asssert all bundles except fragments are ACTIVE.
        for (Bundle b : bundleCtx.getBundles()) {
            System.out.println(String.format("Checking state of bundle [symbolicName=%s, state=%s]",
                b.getSymbolicName(), b.getState()));

            if (b.getHeaders().get(Constants.FRAGMENT_HOST) == null)
                assertTrue(b.getState() == Bundle.ACTIVE);
        }

        // Check that according to the FeaturesService, all Ignite features except ignite-log4j are installed.
        Feature[] features = featuresSvc.getFeatures(IGNITE_FEATURES_NAME_REGEX);

        assertNotNull(features);
        assertEquals(20, features.length);

        for (Feature f : features) {
            if (IGNORED_FEATURES.contains(f.getName()))
                continue;

            boolean installed = featuresSvc.isInstalled(f);

            System.out.println(String.format("Checking if feature is installed [featureName=%s, installed=%s]",
                f.getName(), installed));

            assertTrue(installed);
            assertEquals(PROJECT_VERSION.replaceAll("-", "."), f.getVersion());
        }
    }
}
