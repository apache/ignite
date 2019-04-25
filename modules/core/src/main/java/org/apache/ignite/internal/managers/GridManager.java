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

package org.apache.ignite.internal.managers;

import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;

/**
 * This interface defines life-cycle for kernal manager. Managers provide layer of indirection
 * between kernal and SPI modules. Kernel never calls SPI modules directly but
 * rather calls manager that further delegate the apply to specific SPI module.
 */
@GridToStringExclude
public interface GridManager extends GridComponent {
    /**
     * @return {@code true} if at least one SPI does not have a {@code NO-OP} implementation, {@code false} otherwise.
     */
    public boolean enabled();

    /**
     * This method executed before manager will start SPI.
     */
    public void onBeforeSpiStart();

    /**
     * This method executed after manager started SPI.
     */
    public void onAfterSpiStart();
}