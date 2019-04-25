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

package org.apache.ignite.osgi.activators;

/**
 * Data transfer object representing flags we want to watch from the OSGi tests.
 */
public class TestOsgiFlagsImpl implements TestOsgiFlags {
    /** onBeforeStartInvoked flag. */
    public Boolean onBeforeStartInvoked;

    /** onAfterStartInvoked flag. */
    public Boolean onAfterStartInvoked;

    /** onAfterStartThrowable flag. */
    public Throwable onAfterStartThrowable;

    /** onBeforeStartInvoked flag. */
    public Boolean onBeforeStopInvoked;

    /** onAfterStopInvoked flag. */
    public Boolean onAfterStopInvoked;

    /** onAfterStopThrowable flag. */
    public Throwable onAfterStopThrowable;

    /**
     * @return The flag.
     */
    @Override public Boolean getOnBeforeStartInvoked() {
        return onBeforeStartInvoked;
    }

    /**
     * @return The flag.
     */
    @Override public Boolean getOnAfterStartInvoked() {
        return onAfterStartInvoked;
    }

    /**
     * @return The flag.
     */
    @Override public Throwable getOnAfterStartThrowable() {
        return onAfterStartThrowable;
    }

    /**
     * @return The flag.
     */
    @Override public Boolean getOnBeforeStopInvoked() {
        return onBeforeStopInvoked;
    }

    /**
     * @return The flag.
     */
    @Override public Boolean getOnAfterStopInvoked() {
        return onAfterStopInvoked;
    }

    /**
     * @return The flag.
     */
    @Override public Throwable getOnAfterStopThrowable() {
        return onAfterStopThrowable;
    }
}
