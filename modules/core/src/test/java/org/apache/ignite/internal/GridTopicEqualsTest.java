package org.apache.ignite.internal;

import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.junit.Test;

import static org.junit.Assert.*;

public class GridTopicEqualsTest {
    @Test
    public void T1EqualsTest() {
        Object T1 = GridTopic.TOPIC_AUTH.topic(new IgniteUuid());
        assertEquals("T1", T1.getClass().getSimpleName());
        assertFalse(T1.equals(null));
    }

    @Test
    public void T2EqualsTest() {
        Object T2 = GridTopic.TOPIC_AUTH.topic(new IgniteUuid(), new UUID(1,1));
        assertEquals("T2", T2.getClass().getSimpleName());
        assertFalse(T2.equals(null));
    }

    @Test
    public void T3EqualsTest() {
        Object T3 = GridTopic.TOPIC_AUTH.topic("");
        assertEquals("T3", T3.getClass().getSimpleName());
        assertFalse(T3.equals(null));
    }

    @Test
    public void T4EqualsTest() {
        Object T4 = GridTopic.TOPIC_AUTH.topic("", new UUID(1,1), 1L);
        assertEquals("T4", T4.getClass().getSimpleName());
        assertFalse(T4.equals(null));
    }

    @Test
    public void T5EqualsTest() {
        Object T5 = GridTopic.TOPIC_AUTH.topic("", 1, 1L);
        assertEquals("T5", T5.getClass().getSimpleName());
        assertFalse(T5.equals(null));
    }

    @Test
    public void T6EqualsTest() {
        Object T6 = GridTopic.TOPIC_AUTH.topic("", 1L);
        assertEquals("T6", T6.getClass().getSimpleName());
        assertFalse(T6.equals(null));
    }

    @Test
    public void T7EqualsTest() {
        Object T7 = GridTopic.TOPIC_AUTH.topic("", new UUID(1,1), 1, 1L);
        assertEquals("T7", T7.getClass().getSimpleName());
        assertFalse(T7.equals(null));
    }

    @Test
    public void T8EqualsTest() {
        Object T8 = GridTopic.TOPIC_AUTH.topic(new IgniteUuid(), 1L);
        assertEquals("T8", T8.getClass().getSimpleName());
        assertFalse(T8.equals(null));
    }
}
