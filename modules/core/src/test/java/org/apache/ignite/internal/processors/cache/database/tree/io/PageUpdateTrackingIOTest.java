package org.apache.ignite.internal.processors.cache.database.tree.io;

import java.nio.ByteBuffer;
import junit.framework.TestCase;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.database.tree.io.PageUpdateTrackingIO.*;
import static org.junit.Assert.*;

public class PageUpdateTrackingIOTest extends TestCase {

    public void testBasics() {
        ByteBuffer buf = ByteBuffer.allocate(2048);

        markChanged(buf, 2, 0, 2048);

        assertTrue(wasChanged(buf, 2, 0, 2048));

        assertFalse(wasChanged(buf, 1, 0, 2048));
        assertFalse(wasChanged(buf, 3, 0, 2048));
        assertFalse(wasChanged(buf, 2, 1, 2048));

        System.out.println("countOfPageToTrack(2048) = " + countOfPageToTrack(2048));

    }
}