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

package org.apache.ignite.spi.deployment.uri.tasks;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

/**
 * URI deployment test task which loads Spring bean definitions from spring1.xml configuration file.
 */
public class GridUriDeploymentTestTask1 extends ComputeTaskSplitAdapter<Object, Object> {
    /** */
    @SuppressWarnings({"unchecked", "TypeMayBeWeakened"})
    public GridUriDeploymentTestTask1() {
        XmlBeanFactory factory = new XmlBeanFactory(new ClassPathResource("spring1.xml", getClass().getClassLoader()));

        factory.setBeanClassLoader(getClass().getClassLoader());

        Map map = (Map)factory.getBean("task.cfg");

        System.out.println("Loaded data from spring1.xml [map=" + map + ']');

        assert map != null;

        GridUriDeploymentDependency1 depend = new GridUriDeploymentDependency1();

        System.out.println("GridUriDeploymentTestTask1 dependency resolved [msg=" + depend.getMessage() + ']');
    }

    /**
     * {@inheritDoc}
     */
    @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Object reduce(List<ComputeJobResult> results) {
        return null;
    }
}