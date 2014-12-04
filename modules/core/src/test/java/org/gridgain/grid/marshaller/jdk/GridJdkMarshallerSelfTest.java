/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.marshaller.jdk;

import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.jdk.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.testframework.junits.common.*;

/**
 * JDK marshaller self test.
 */
@SuppressWarnings({"JUnitTestCaseWithNoTests"})
@GridCommonTest(group = "Marshaller")
public class GridJdkMarshallerSelfTest extends GridMarshallerAbstractTest {
    /** {@inheritDoc} */
    @Override protected GridMarshaller createMarshaller() {
        return new IgniteJdkMarshaller();
    }
}
