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

package org.apache.ignite.testframework.junits.spi;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for SPI configuration tests.
 * @param <T> Type of tested SPI.
 */
public abstract class GridSpiAbstractConfigTest<T extends IgniteSpi> extends GridSpiAbstractTest<T> {
    /** Default constructor. */
    protected GridSpiAbstractConfigTest() {
        super(false);
    }

    /**
     * Checks that unacceptable property value prevents SPI from being started.
     *
     * @param spi Spi to test property on.
     * @param propName name of property to check.
     * @param val An illegal value.
     * @throws Exception If check failed.
     */
    protected void checkNegativeSpiProperty(IgniteSpi spi, String propName, @Nullable Object val) throws Exception {
        checkNegativeSpiProperty(spi, propName, val, true);
    }

    /**
     * Checks that unacceptable property value prevents SPI from being started.
     *
     * @param spi Spi to test property on.
     * @param propName name of property to check.
     * @param val An illegal value.
     * @param checkExMsg If {@code true} then additional info will be added to failure.
     * @throws Exception If check failed.
     */
    protected void checkNegativeSpiProperty(IgniteSpi spi, String propName, Object val, boolean checkExMsg)
        throws Exception {
        assert spi != null;
        assert propName != null;

        getTestData().getTestResources().inject(spi);

        String mtdName = "set" + propName.substring(0, 1).toUpperCase() + propName.substring(1);

        Method mtd = null;

        for (Method m : spi.getClass().getMethods())
            if (m.getName().equals(mtdName)) {
                mtd = m;

                break;
            }

        assert mtd != null : "The setter is not found for property: " + propName;

        boolean err = false;

        try {
            mtd.invoke(spi, val);
        }
        catch (InvocationTargetException e) {
            info("SPI property setter thrown exception: " + e);

            if (e.getCause() instanceof IllegalArgumentException)
                err = true;
            else
                throw e;
        }

        if (!err)
            try {
                if (!(spi instanceof DiscoverySpi))
                    spi.getNodeAttributes();

                spi.spiStart(getTestIgniteInstanceName());
            }
            catch (IgniteSpiException e) {
                info("SPI start thrown exception: " + e);

                if (checkExMsg)
                    assert e.getMessage().contains("SPI parameter failed condition check: ") :
                        "SPI has returned wrong exception message [propName=" + propName + ", msg=" + e + ']';

                err = true;
            }

        assert err : "No check for property [property=" + propName +", value=" + val + ']';
    }
}