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

package org.apache.ignite.console.event;

import org.springframework.context.ApplicationEvent;
import org.springframework.core.ResolvableType;
import org.springframework.core.ResolvableTypeProvider;

/**
 * Generic application event with payload.
 */
public class Event<T> extends ApplicationEvent implements ResolvableTypeProvider {
    /** Type. */
    private EventType evtType;

    /** Source class. */
    private Class<T> srcCls;

    /** Event type class. */
    private Class<? extends EventType> evtTypeCls;

    /**
     * @param evtType Event type.
     * @param payload Payload.
     */
    public Event(EventType evtType, T payload) {
        super(payload);

        this.evtType = evtType;
        srcCls = (Class<T>) payload.getClass();
        evtTypeCls = evtType.getClass();
    }

    /**
     * @return Type.
     */
    public EventType getType() {
        return evtType;
    }

    /** {@inheritDoc} */
    @Override public T getSource() {
        return (T) super.getSource();
    }

    /** {@inheritDoc} */
    @Override public ResolvableType getResolvableType() {
        return ResolvableType.forClassWithGenerics(Event.class, srcCls);
    }
}
