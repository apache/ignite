

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
