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

package org.test.gridify;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.ignite.compute.gridify.Gridify;

/**
 * AOP target.
 */
public class ExternalAopTarget {
    /**
     * @param arg Argument.
     * @return Argument parsed to integer.
     */
    @Gridify(igniteInstanceName="ExternalAopTarget")
    public int gridifyDefault(String arg) {
        return Integer.parseInt(arg);
    }

    /**
     * @param arg Argument.
     * @return Argument parsed to integer.
     */
    @Gridify(igniteInstanceName="ExternalAopTarget", taskClass = ExternalGridifyTask.class)
    public int gridifyNonDefaultClass(String arg) {
        return Integer.parseInt(arg);
    }


    /**
     * @param arg Argument.
     * @return Argument parsed to integer.
     */
    @Gridify(igniteInstanceName="ExternalAopTarget", taskName = ExternalGridifyTask.TASK_NAME)
    public int gridifyNonDefaultName(String arg) {
        return Integer.parseInt(arg);
    }

    /**
     * @param arg Argument.
     * @return ALways 0.
     */
    @Gridify(igniteInstanceName="ExternalAopTarget", taskName = "myTask", taskClass = ExternalGridifyTask.class)
    public int gridifyTaskClassAndTaskName(String arg) {
        assert arg != null;

        return 0;
    }

    /**
     * @param arg Argument.
     * @return No-op.
     * @throws ExternalGridifyException Always.
     */
    @Gridify(igniteInstanceName="ExternalAopTarget")
    public int gridifyDefaultException(String arg) throws ExternalGridifyException {
        throw new ExternalGridifyException(arg);
    }

    /**
     * @param arg Argument.
     * @return Argument parsed to integer.
     * @throws ExternalGridifyException If failed.
     */
    @Gridify(igniteInstanceName="ExternalAopTarget")
    public int gridifyDefaultResource(String arg) throws ExternalGridifyException {
        getResource();

        return Integer.parseInt(arg);
    }

    /**
     * @param arg Argument.
     * @return Argument parsed to integer.
     * @throws ExternalGridifyException If failed.
     */
    @Gridify(igniteInstanceName="ExternalAopTarget", taskClass = ExternalGridifyTask.class)
    public int gridifyNonDefaultClassResource(String arg) throws ExternalGridifyException {
        getResource();

        return Integer.parseInt(arg);
    }


    /**
     * @param arg Argument.
     * @return Argument parsed to integer.
     * @throws ExternalGridifyException If failed.
     */
    @Gridify(igniteInstanceName="ExternalAopTarget", taskName = ExternalGridifyTask.TASK_NAME)
    public int gridifyNonDefaultNameResource(String arg) throws ExternalGridifyException {
        getResource();

        return Integer.parseInt(arg);
    }

    /**
     * @throws ExternalGridifyException If failed.
     */
    private void getResource() throws ExternalGridifyException {
        try (InputStream in = getClass().getResourceAsStream("test_resource.properties")) {
            assert in != null;

            Properties prop = new Properties();

            prop.load(in);

            String val = prop.getProperty("param1");

            assert "1".equals(val);
        }
        catch (IOException e) {
            throw new ExternalGridifyException("Failed to test load properties file.", e);
        }
    }
}