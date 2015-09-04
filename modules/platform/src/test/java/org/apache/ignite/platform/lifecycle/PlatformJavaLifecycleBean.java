/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.platform.lifecycle;

import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;

/**
 * Java lifecycle bean.
 */
public class PlatformJavaLifecycleBean implements LifecycleBean {
    /** Count of "beforeStart" invocations. */
    public static volatile int beforeStartCnt;

    /** Count of "afterStart" invocations. */
    public static volatile int afterStartCnt;

    /** {@inheritDoc} */
    @Override public void onLifecycleEvent(LifecycleEventType evt) {
        switch (evt) {
            case BEFORE_NODE_START:
                beforeStartCnt++;

                break;

            case AFTER_NODE_START:
                afterStartCnt++;

                break;
        }
    }
}