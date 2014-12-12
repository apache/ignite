/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri.tasks;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.springframework.beans.factory.xml.*;
import org.springframework.core.io.*;

import java.util.*;

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
    @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteCheckedException {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        return null;
    }
}
