/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework.junits.spi;

import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;

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

                spi.spiStart(getTestGridName());
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
