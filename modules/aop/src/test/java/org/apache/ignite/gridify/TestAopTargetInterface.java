/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.gridify;

import java.io.Serializable;
import org.apache.ignite.compute.gridify.Gridify;

/**
 * Test AOP target interface.
 */
public interface TestAopTargetInterface extends Serializable {
    /**
     * @param arg Argument.
     * @return Result.
     */
    @Gridify(igniteInstanceName="TestAopTargetInterface")
    public int gridifyDefault(String arg);

    /**
     * @param arg Argument.
     * @return Result.
     */
    @Gridify(igniteInstanceName="TestAopTargetInterface", taskName = TestGridifyTask.TASK_NAME)
    public int gridifyNonDefaultName(String arg);

    /**
     * @param arg Argument.
     * @return Result.
     */
    @Gridify(igniteInstanceName="TestAopTargetInterface", taskClass = TestGridifyTask.class)
    public int gridifyNonDefaultClass(String arg);

    /**
     * @param arg Argument.
     * @return Result.
     */
    @Gridify(igniteInstanceName="TestAopTargetInterface", taskName = "")
    public int gridifyNoName(String arg);

    /**
     * @param arg Argument.
     * @return Result.
     * @throws TestGridifyException If failed.
     */
    @Gridify(igniteInstanceName="TestAopTargetInterface")
    public int gridifyDefaultException(String arg) throws TestGridifyException;

    /**
     * @param arg Argument.
     * @return Result.
     * @throws TestGridifyException If failed.
     */
    @Gridify(igniteInstanceName="TestAopTargetInterface")
    public int gridifyDefaultResource(String arg) throws TestGridifyException;

    /**
     * @param arg Argument.
     * @return Result.
     * @throws TestGridifyException If failed.
     */
    @Gridify(igniteInstanceName="TestAopTargetInterface", taskClass = TestGridifyTask.class)
    public int gridifyNonDefaultClassResource(String arg) throws TestGridifyException;

    /**
     * @param arg Argument.
     * @return Result.
     * @throws TestGridifyException If failed.
     */
    @Gridify(igniteInstanceName="TestAopTargetInterface", taskName = TestGridifyTask.TASK_NAME)
    public int gridifyNonDefaultNameResource(String arg) throws TestGridifyException;
}