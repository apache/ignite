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
import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.osgi.activators.BasicIgniteTestActivator;
import org.apache.ignite.osgi.activators.TestOsgiFlags;
import org.apache.ignite.osgi.activators.TestOsgiFlagsImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.ops4j.pax.exam.util.Filter;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.ops4j.pax.exam.CoreOptions.streamBundle;
import static org.ops4j.pax.tinybundles.core.TinyBundles.bundle;
import static org.ops4j.pax.tinybundles.core.TinyBundles.withBnd;

/**
 * Pax Exam test class to check whether the Ignite service is exposed properly and whether lifecycle callbacks
 * are invoked.
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class IgniteOsgiServiceTest extends AbstractIgniteKarafTest {
    /** Injects the Ignite OSGi service. */
    @Inject @Filter("(ignite.name=testGrid)")
    private Ignite ignite;

    @Inject
    private BundleContext bundleCtx;

    /**
     * @return Config.
     */
    @Configuration
    public Option[] bundleDelegatingConfig() {
        List<Option> options = new ArrayList<>(Arrays.asList(baseConfig()));

        // Add bundles we require.
        options.add(
            streamBundle(bundle()
                .add(BasicIgniteTestActivator.class)
                .add(TestOsgiFlags.class)
                .add(TestOsgiFlagsImpl.class)
                .set(Constants.BUNDLE_SYMBOLICNAME, BasicIgniteTestActivator.class.getSimpleName())
                .set(Constants.BUNDLE_ACTIVATOR, BasicIgniteTestActivator.class.getName())
                .set(Constants.EXPORT_PACKAGE, "org.apache.ignite.osgi.activators")
                .set(Constants.DYNAMICIMPORT_PACKAGE, "*")
                .build(withBnd())));

        // Uncomment this if you'd like to debug inside the container.
        // options.add(KarafDistributionOption.debugConfiguration());

        return CoreOptions.options(options.toArray(new Option[0]));
    }

    /**
     * Builds the probe.
     *
     * @param probe The probe builder.
     * @return The probe builder.
     */
    @ProbeBuilder
    public TestProbeBuilder probeConfiguration(TestProbeBuilder probe) {
        probe.setHeader(Constants.IMPORT_PACKAGE, "*,org.apache.ignite.osgi.activators;resolution:=\"optional\"");

        return probe;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServiceExposedAndCallbacksInvoked() throws Exception {
        assertNotNull(ignite);
        assertEquals("testGrid", ignite.name());

        TestOsgiFlags flags = (TestOsgiFlags) bundleCtx.getService(
            bundleCtx.getAllServiceReferences(TestOsgiFlags.class.getName(), null)[0]);

        assertNotNull(flags);
        assertEquals(Boolean.TRUE, flags.getOnBeforeStartInvoked());
        assertEquals(Boolean.TRUE, flags.getOnAfterStartInvoked());

        // The bundle is still not stopped, therefore these callbacks cannot be tested.
        assertNull(flags.getOnBeforeStopInvoked());
        assertNull(flags.getOnAfterStopInvoked());

        // No exceptions.
        assertNull(flags.getOnAfterStartThrowable());
        assertNull(flags.getOnAfterStopThrowable());
    }

    /**
     * @return Features.
     */
    @Override protected List<String> featuresToInstall() {
        return Arrays.asList("ignite-core");
    }
}
