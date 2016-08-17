package org.apache.ignite.testframework.assertions;

/**
 * An {@link Assertion} is a condition that is expected to be true. Failing that, an implementation should throw an
 * {@link AssertionError} or specialized subclass containing information about what the assertion failed.
 */
public interface Assertion {
    /**
     * Test that some condition has been satisfied.
     *
     * @throws AssertionError if the condition was not satisfied.
     */
    void test() throws AssertionError;
}
