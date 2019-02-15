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

package org.apache.ignite.spi.deployment.uri;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.IgniteSpiException;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.xml.XmlBeanFactory;

/**
 * Helper class which helps to read deployer and tasks information from
 * {@code Spring} configuration file.
 */
class GridUriDeploymentSpringDocument {
    /** Initialized springs beans factory. */
    private final XmlBeanFactory factory;

    /** List of tasks from GAR description. */
    private List<Class<? extends ComputeTask<?, ?>>> tasks;

    /**
     * Creates new instance of configuration helper with given configuration.
     *
     * @param factory Configuration factory.
     */
    GridUriDeploymentSpringDocument(XmlBeanFactory factory) {
        assert factory != null;

        this.factory = factory;
    }

    /**
     * Loads tasks declared in configuration by given class loader.
     *
     * @param clsLdr Class loader.
     * @return Declared tasks.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown if there are no tasks in
     *      configuration or configuration could not be read.
     */
    @SuppressWarnings({"unchecked"})
    List<Class<? extends ComputeTask<?, ?>>> getTasks(ClassLoader clsLdr) throws IgniteSpiException {
        assert clsLdr!= null;

        try {
            if (tasks == null) {
                tasks = new ArrayList<>();

                Map<String, List> beans = factory.getBeansOfType(List.class);

                if (!beans.isEmpty()) {
                    for (List<String> list : beans.values()) {
                        for (String clsName : list) {
                            Class taskCls;

                            try {
                                taskCls = clsLdr.loadClass(clsName);
                            }
                            catch (ClassNotFoundException e) {
                                throw new IgniteSpiException("Failed to load task class [className=" + clsName + ']', e);
                            }

                            assert taskCls != null;

                            tasks.add(taskCls);
                        }
                    }
                }
            }
        }
        catch (BeansException e) {
            throw new IgniteSpiException("Failed to get tasks declared in XML file.", e);
        }

        return tasks;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUriDeploymentSpringDocument.class, this);
    }
}