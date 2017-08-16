package org.apache.ignite.examples;

import org.apache.ignite.examples.streaming.StreamingExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;

/**
 * Streaming example self test.
 */
public class StreamingExampleSelfTest extends GridAbstractExamplesTest {

    /**
     * @throws Exception If failed.
     */
    public void testStreamingExample() throws Exception {
        StreamingExample.main(EMPTY_ARGS);
    }

}