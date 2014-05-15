/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.test.gridify;

import org.gridgain.grid.compute.gridify.*;

import java.io.*;
import java.util.*;

/**
 * AOP target.
 */
public class GridExternalAopTarget {
    /**
     * @param arg Argument.
     * @return Argument parsed to integer.
     */
    @Gridify(gridName="GridExternalAopTarget")
    public int gridifyDefault(String arg) {
        return Integer.parseInt(arg);
    }

    /**
     * @param arg Argument.
     * @return Argument parsed to integer.
     */
    @Gridify(gridName="GridExternalAopTarget", taskClass = GridExternalGridifyTask.class)
    public int gridifyNonDefaultClass(String arg) {
        return Integer.parseInt(arg);
    }


    /**
     * @param arg Argument.
     * @return Argument parsed to integer.
     */
    @Gridify(gridName="GridExternalAopTarget", taskName = GridExternalGridifyTask.TASK_NAME)
    public int gridifyNonDefaultName(String arg) {
        return Integer.parseInt(arg);
    }

    /**
     * @param arg Argument.
     * @return ALways 0.
     */
    @Gridify(gridName="GridExternalAopTarget", taskName = "myTask", taskClass = GridExternalGridifyTask.class)
    public int gridifyTaskClassAndTaskName(String arg) {
        assert arg != null;

        return 0;
    }

    /**
     * @param arg Argument.
     * @return No-op.
     * @throws GridExternalGridifyException Always.
     */
    @Gridify(gridName="GridExternalAopTarget")
    public int gridifyDefaultException(String arg) throws GridExternalGridifyException {
        throw new GridExternalGridifyException(arg);
    }

    /**
     * @param arg Argument.
     * @return Argument parsed to integer.
     * @throws GridExternalGridifyException If failed.
     */
    @Gridify(gridName="GridExternalAopTarget")
    public int gridifyDefaultResource(String arg) throws GridExternalGridifyException {
        getResource();

        return Integer.parseInt(arg);
    }

    /**
     * @param arg Argument.
     * @return Argument parsed to integer.
     * @throws GridExternalGridifyException If failed.
     */
    @Gridify(gridName="GridExternalAopTarget", taskClass = GridExternalGridifyTask.class)
    public int gridifyNonDefaultClassResource(String arg) throws GridExternalGridifyException {
        getResource();

        return Integer.parseInt(arg);
    }


    /**
     * @param arg Argument.
     * @return Argument parsed to integer.
     * @throws GridExternalGridifyException If failed.
     */
    @Gridify(gridName="GridExternalAopTarget", taskName = GridExternalGridifyTask.TASK_NAME)
    public int gridifyNonDefaultNameResource(String arg) throws GridExternalGridifyException {
        getResource();

        return Integer.parseInt(arg);
    }

    /**
     * @throws GridExternalGridifyException If failed.
     */
    private void getResource() throws GridExternalGridifyException {
        InputStream in = getClass().getResourceAsStream("test_resource.properties");

        assert in != null;

        Properties prop = new Properties();

        try {
            prop.load(in);
        }
        catch (IOException e) {
            throw new GridExternalGridifyException("Failed to test load properties file.", e);
        }

        String val = prop.getProperty("param1");

        assert "1".equals(val);
    }
}
