/*
 *                    GridGain Community Edition Licensing
 *                    Copyright 2019 GridGain Systems, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 *  Restriction; you may not use this file except in compliance with the License. You may obtain a
 *  copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 *  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the specific language governing permissions
 *  and limitations under the License.
 *
 *  Commons Clause Restriction
 *
 *  The Software is provided to you by the Licensor under the License, as defined below, subject to
 *  the following condition.
 *
 *  Without limiting other conditions in the License, the grant of rights under the License will not
 *  include, and the License does not grant to you, the right to Sell the Software.
 *  For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 *  under the License to provide to third parties, for a fee or other consideration (including without
 *  limitation fees for hosting or consulting/ support services related to the Software), a product or
 *  service whose value derives, entirely or substantially, from the functionality of the Software.
 *  Any license notice or attribution required by the License must also include this Commons Clause
 *  License Condition notice.
 *
 *  For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 *  the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 *  Edition software provided with this notice.
 */

package org.apache.ignite.spi.deployment.uri.tasks;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.support.AbstractBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

/**
 * This class defines grid task for this example. Grid task is responsible for splitting the task into jobs. This
 * particular implementation splits given string into individual words and creates grid jobs for each word. Task class
 * in that example should be placed in GAR file.
 */
@ComputeTaskName("GarHelloWorldTask")
public class GarHelloWorldTask extends ComputeTaskSplitAdapter<String, String> {
    /** {@inheritDoc} */
    @Override public Collection<? extends ComputeJob> split(int gridSize, String arg) throws IgniteException {
        // Create Spring context.
        AbstractBeanFactory fac = new XmlBeanFactory(
            new ClassPathResource("org/apache/ignite/spi/deployment/uri/tasks/gar-spring-bean.xml", getClass().getClassLoader()));

        fac.setBeanClassLoader(getClass().getClassLoader());

        // Load imported bean from GAR/lib folder.
        GarHelloWorldBean bean = (GarHelloWorldBean)fac.getBean("example.bean");

        String msg = bean.getMessage(arg);

        assert msg != null;

        // Split the passed in phrase into multiple words separated by spaces.
        List<String> words = Arrays.asList(msg.split(" "));

        Collection<ComputeJob> jobs = new ArrayList<>(words.size());

        // Use imperative OOP APIs.
        for (String word : words) {
            // Every job gets its own word as an argument.
            jobs.add(new ComputeJobAdapter(word) {
                /*
                 * Simply prints the job's argument.
                 */
                @Nullable @Override public Serializable execute() {
                    System.out.println(">>>");
                    System.out.println(">>> Printing '" + argument(0) + "' on this node from grid job.");
                    System.out.println(">>>");

                    // This job does not return any result.
                    return null;
                }
            });
        }

        return jobs;
    }

    @Nullable @Override public String reduce(List<ComputeJobResult> results) throws IgniteException {
        return String.valueOf(results.size());
    }
}