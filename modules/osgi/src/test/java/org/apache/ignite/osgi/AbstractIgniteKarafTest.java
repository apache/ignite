/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.osgi;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import org.apache.karaf.features.FeaturesService;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.karaf.options.LogLevelOption;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.osgi.framework.BundleContext;

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
 * Abstract test class that sets up an Apache Karaf container with Ignite installed.
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public abstract class AbstractIgniteKarafTest {
    /** Features we do not expect to be installed. */
    protected static final Set<String> IGNORED_FEATURES = new HashSet<>(
        Arrays.asList("ignite-log4j", "ignite-scalar-2.10"));

    /** Regex matching ignite features. */
    protected static final String IGNITE_FEATURES_NAME_REGEX = "ignite.*";

    /** Project version. */
    protected static final String PROJECT_VERSION = System.getProperty("projectVersion");

    /** Pax Exam will inject the Bundle Context here. */
    @Inject
    protected BundleContext bundleCtx;

    /** Pax Exam will inject the Karaf Features Service. */
    @Inject
    protected FeaturesService featuresSvc;

    /**
     * Base configuration for a Karaf container running the specified Ignite features.
     *
     * @return The configuration.
     */
    public Option[] baseConfig() {
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
                featuresToInstall().toArray(new String[0])),

            // Propagate the projectVersion system property.
            systemProperty("projectVersion").value(System.getProperty("projectVersion"))
        );
    }

    protected abstract List<String> featuresToInstall();
}
