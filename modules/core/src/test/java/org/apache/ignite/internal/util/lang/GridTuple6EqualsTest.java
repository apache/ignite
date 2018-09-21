package org.apache.ignite.internal.util.lang;

import org.junit.Test;

import static org.junit.Assert.*;

public class GridTuple6EqualsTest {
    @Test
    public void equalsTest(){
        GridTuple6 obj = new GridTuple6();
        assertFalse(obj.equals(new GridTuple5()));
    }
}
