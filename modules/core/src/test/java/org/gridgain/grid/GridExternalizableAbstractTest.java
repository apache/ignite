/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.jdk.*;
import org.apache.ignite.marshaller.optimized.*;
import org.gridgain.testframework.junits.common.*;
import java.util.*;

/**
 * Base externalizable test class.
 */
public class GridExternalizableAbstractTest extends GridCommonAbstractTest {
    /**
     * @return Marshallers.
     */
    protected List<IgniteMarshaller> getMarshallers() {
        List<IgniteMarshaller> marshallers = new ArrayList<>();

        marshallers.add(new IgniteJdkMarshaller());
        marshallers.add(new IgniteOptimizedMarshaller());

        return marshallers;
    }
}
