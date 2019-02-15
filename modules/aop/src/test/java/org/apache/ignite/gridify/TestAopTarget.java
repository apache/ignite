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

package org.apache.ignite.gridify;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.ignite.compute.gridify.Gridify;

/**
 * AOP test.
 */
public class TestAopTarget implements TestAopTargetInterface {
    /**
     * @param arg Argument.
     * @return Result.
     */
    @Gridify(igniteInstanceName="TestAopTarget")
    @Override public int gridifyDefault(String arg) {
        return Integer.parseInt(arg);
    }

    /**
     * @param arg Argument.
     * @return Result.
     */
    @Gridify(igniteInstanceName="TestAopTarget", taskClass = TestGridifyTask.class)
    @Override public int gridifyNonDefaultClass(String arg) {
        return Integer.parseInt(arg);
    }


    /**
     * @param arg Argument.
     * @return Result.
     */
    @Gridify(igniteInstanceName="TestAopTarget", taskName = TestGridifyTask.TASK_NAME)
    @Override public int gridifyNonDefaultName(String arg) {
        return Integer.parseInt(arg);
    }

    /**
     * @param arg Argument.
     * @return Result.
     */
    @Gridify(igniteInstanceName="TestAopTarget", taskName = "")
    @Override public int gridifyNoName(String arg) {
        return 0;
    }

    /**
     * @param arg Argument.
     * @return Result.
     * @throws TestGridifyException If failed.
     */
    @Gridify(igniteInstanceName="TestAopTarget")
    @Override public int gridifyDefaultException(String arg) throws TestGridifyException {
        throw new TestGridifyException(arg);
    }

    /**
     * @param arg Argument.
     * @return Result.
     * @throws TestGridifyException If failed.
     */
    @Gridify(igniteInstanceName="TestAopTarget")
    @Override public int gridifyDefaultResource(String arg) throws TestGridifyException {
        int res = Integer.parseInt(arg);

        Integer rsrcVal = getResource();

        assert rsrcVal != null;
        assert rsrcVal == res : "Invalid result [res=" + res + ", rsrc=" + rsrcVal + ']';

        return res;
    }

    /**
     * @param arg Argument.
     * @return Result.
     * @throws TestGridifyException If failed.
     */
    @Gridify(igniteInstanceName="TestAopTarget", taskClass = TestGridifyTask.class)
    @Override public int gridifyNonDefaultClassResource(String arg) throws TestGridifyException {
        assert getResource() != null;

        return Integer.parseInt(arg);
    }


    /**
     * @param arg Argument.
     * @return Result.
     * @throws TestGridifyException If failed.
     */
    @Gridify(igniteInstanceName="TestAopTarget", taskName = TestGridifyTask.TASK_NAME)
    @Override public int gridifyNonDefaultNameResource(String arg) throws TestGridifyException {
        assert getResource() != null;

        return Integer.parseInt(arg);
    }

    /**
     * @return Result.
     * @throws TestGridifyException If failed.
     */
    private Integer getResource() throws TestGridifyException {
        try (InputStream in = getClass().getResourceAsStream("test_resource.properties")) {
            assert in != null;

            Properties prop = new Properties();

            prop.load(in);

            String val = prop.getProperty("param1");

            return Integer.parseInt(val);
        }
        catch (IOException e) {
            throw new TestGridifyException("Failed to test load properties file.", e);
        }
    }
}