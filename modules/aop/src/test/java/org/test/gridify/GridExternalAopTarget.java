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

package org.test.gridify;

import org.apache.ignite.compute.gridify.*;

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
        try (InputStream in = getClass().getResourceAsStream("test_resource.properties")) {
            assert in != null;

            Properties prop = new Properties();

            prop.load(in);

            String val = prop.getProperty("param1");

            assert "1".equals(val);
        }
        catch (IOException e) {
            throw new GridExternalGridifyException("Failed to test load properties file.", e);
        }
    }
}
