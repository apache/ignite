package org.apache.ignite.stream.callable;

import org.apache.ignite.lang.IgniteCallable;

/**
 * Test callable for GridSerializationTest
 */
public class GridSimpleCallable implements IgniteCallable<String> {

    /**
     * @return alwais return "test" string
     */
    @Override
    public String call() throws Exception {
        System.out.println("HERE");
        return "here";
    }
}

