/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri.tasks;

import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.springframework.beans.factory.xml.*;
import org.springframework.core.io.*;
import java.util.*;

/**
 * URI deployment test task which loads Spring bean definitions from spring1.xml configuration file.
 */
public class GridUriDeploymentTestTask2 extends ComputeTaskSplitAdapter<Object, Object> {
    /** */
    private static final long serialVersionUID = 172455091783232848L;

    /** */
    @SuppressWarnings("unchecked")
    public GridUriDeploymentTestTask2() {
        XmlBeanFactory factory = new XmlBeanFactory(
            new ClassPathResource("org/gridgain/grid/spi/deployment/uri/tasks/spring2.xml",
                getClass().getClassLoader()));

        factory.setBeanClassLoader(getClass().getClassLoader());

        Map map = (Map)factory.getBean("task.cfg");

        System.out.println("Loaded data from spring2.xml [map=" + map + ']');

        assert map != null;

        GridUriDeploymentDependency2 depend = new GridUriDeploymentDependency2();

        System.out.println("GridUriDeploymentTestTask2 dependency resolved [msg=" + depend.getMessage() + ']');
    }

    /**
     * {@inheritDoc}
     */
    @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) throws GridException {
        System.out.println("Split is called: " + this);

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
        System.out.println("Reduce is called.");

        return null;
    }
}
