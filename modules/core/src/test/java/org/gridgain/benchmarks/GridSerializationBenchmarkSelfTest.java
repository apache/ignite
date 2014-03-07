/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.benchmarks;

import org.gridgain.benchmarks.serialization.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Test for {@link GridSerializationBenchmark}.
 */
public class GridSerializationBenchmarkSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If error occurs.
     */
    public void testGridSerializationBenchmark() throws Exception {
        GridSerializationBenchmark.main(EMPTY_ARGS);
    }
}
