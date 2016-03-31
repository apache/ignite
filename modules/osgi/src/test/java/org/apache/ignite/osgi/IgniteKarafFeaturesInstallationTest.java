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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.karaf.features.Feature;
import org.junit.Test;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.karaf.options.KarafDistributionOption;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Pax Exam test class to check if all features could be resolved and installed.
 */
public class IgniteKarafFeaturesInstallationTest extends AbstractIgniteKarafTest {
    /** Number of features expected to exist. */
    private static final int EXPECTED_FEATURES = 25;

    private static final String CAMEL_REPO_URI = "mvn:org.apache.camel.karaf/apache-camel/" +
        System.getProperty("camelVersion") + "/xml/features";

    /**
     * Container configuration.
     *
     * @return The configuration.
     */
    @Configuration
    public Option[] config() {
        List<Option> options = new ArrayList<>(Arrays.asList(baseConfig()));

        options.add(KarafDistributionOption.features(CAMEL_REPO_URI));

        return CoreOptions.options(options.toArray(new Option[0]));
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
        assertEquals(EXPECTED_FEATURES, features.length);

        for (Feature f : features) {
            if (IGNORED_FEATURES.contains(f.getName()))
                continue;

            boolean installed = featuresSvc.isInstalled(f);

            System.out.println(String.format("Checking if feature is installed [featureName=%s, installed=%s]",
                f.getName(), installed));

            assertTrue(installed);
            assertEquals(PROJECT_VERSION.replaceAll("-", "."), f.getVersion().replaceAll("-", "."));
        }
    }

    /**
     * @return Features list.
     */
    @Override protected List<String> featuresToInstall() {
        return Arrays.asList("ignite-all", "ignite-hibernate");
    }
}
