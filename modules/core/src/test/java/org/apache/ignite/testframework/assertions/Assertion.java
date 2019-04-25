/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    public void test() throws AssertionError;
}
