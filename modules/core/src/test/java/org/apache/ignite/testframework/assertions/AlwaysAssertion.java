package org.apache.ignite.testframework.assertions;

/** An {@link Assertion} that always passes. */
public class AlwaysAssertion implements Assertion {

    /** Singleton instance */
    public static final Assertion INSTANCE = new AlwaysAssertion();

    @Override public void test() throws AssertionError {
    }
}
