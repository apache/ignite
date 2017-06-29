package org.apache.ignite.examples;

/**
 * Created by Ivanan on 29.06.2017.
 */
import org.apache.ignite.examples.streaming.StreamingExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;

/**
 * ClassLoader zero deployment example self test.
 */
public class StreamingExampleSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    public void testStreamingExample() throws Exception {
        StreamingExample.main(EMPTY_ARGS);
    }
}
