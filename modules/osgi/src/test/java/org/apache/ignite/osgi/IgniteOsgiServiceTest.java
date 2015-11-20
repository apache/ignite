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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.ops4j.pax.exam.util.Filter;
import org.osgi.framework.Constants;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.ops4j.pax.exam.CoreOptions.streamBundle;
import static org.ops4j.pax.tinybundles.core.TinyBundles.bundle;
import static org.ops4j.pax.tinybundles.core.TinyBundles.withBnd;

/**
 * Pax Exam test class to check whether the Ignite service is exposed properly.
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class IgniteOsgiServiceTest extends AbstractIgniteKarafTest {

    /** Injects the Ignite OSGi service. */
    @Inject @Filter("(ignite.name=testGrid)")
    private Ignite ignite;

    /**
     * @return
     */
    @Configuration
    public Option[] bundleDelegatingConfig() {
        List<Option> options = new ArrayList<>(Arrays.asList(baseConfig()));

        // Add bundles we require.
        options.add(
            streamBundle(bundle()
            .add(BasicIgniteTestActivator.class)
            .set(Constants.BUNDLE_SYMBOLICNAME, BasicIgniteTestActivator.class.getSimpleName())
            .set(Constants.BUNDLE_ACTIVATOR, BasicIgniteTestActivator.class.getName())
            .set(Constants.DYNAMICIMPORT_PACKAGE, "*")
            .build(withBnd())));

        return CoreOptions.options(options.toArray(new Option[0]));
    }

    /**
     * @throws Exception
     */
    @Test
    public void testServiceExposed() throws Exception {
        assertNotNull(ignite);
        assertEquals("testGrid", ignite.name());
    }

    /**
     * @return
     */
    @Override protected List<String> featuresToInstall() {
        return Arrays.asList("ignite-core");
    }

}
