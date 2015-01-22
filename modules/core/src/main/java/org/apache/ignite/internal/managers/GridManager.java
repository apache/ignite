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

package org.apache.ignite.internal.managers;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.*;
import org.apache.ignite.internal.util.tostring.*;

import java.util.*;

/**
 * This interface defines life-cycle for kernal manager. Managers provide layer of indirection
 * between kernal and SPI modules. Kernel never calls SPI modules directly but
 * rather calls manager that further delegate the apply to specific SPI module.
 */
@GridToStringExclude
public interface GridManager extends GridComponent {
    /**
     * Adds attributes from underlying SPI to map of all attributes.
     *
     * @param attrs Map of all attributes gotten from SPI's so far.
     * @throws IgniteCheckedException Wrapper for exception thrown by underlying SPI.
     */
    public void addSpiAttributes(Map<String, Object> attrs) throws IgniteCheckedException;

    /**
     * @return Returns {@code true} if at least one SPI does not have a {@code NO-OP}
     *      implementation, {@code false} otherwise.
     */
    public boolean enabled();
}
