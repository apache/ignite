package org.gridgain.grid;

import junit.framework.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.util.GridUtils.*;

/**
 * Tests for proper link output in stack traces.
 */
public class GridExceptionHelpLinksSelfTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    public void testDefaultLinks() throws Exception {
        assertTrue(hasLinksInMessage(new GridException("test"), DFLT_HELP_LINKS));
        assertTrue(hasLinksInMessage(new GridException(new Exception()), DFLT_HELP_LINKS));
        assertTrue(hasLinksInMessage(new GridException("test", new Exception()), DFLT_HELP_LINKS));

        assertTrue(hasLinksInMessage(new GridRuntimeException("test"), DFLT_HELP_LINKS));
        assertTrue(hasLinksInMessage(new GridRuntimeException(new Exception()), DFLT_HELP_LINKS));
        assertTrue(hasLinksInMessage(new GridRuntimeException("test", new Exception()), DFLT_HELP_LINKS));
    }

    /**
     * Tests default links suppression.
     */
    public void testLinksUniqueness() {
        assertLinksAppearOnce(
            new GridException("test",
                new GridException("test nested",
                    new GridException("last"))),
            DFLT_HELP_LINKS);

        assertLinksAppearOnce(
            new GridRuntimeException("test",
                new GridRuntimeException("test nested",
                    new GridRuntimeException("last"))),
            DFLT_HELP_LINKS);

        assertLinksAppearOnce(
            new GridException("test",
                new GridRuntimeException("test nested",
                    new GridException("last"))),
            DFLT_HELP_LINKS);

        assertLinksAppearOnce(
            new GridRuntimeException("test",
                new GridException("test nested",
                    new GridRuntimeException("last"))),
            DFLT_HELP_LINKS);
    }

    /**
     * @param e Root exception.
     * @param links Set of links to ensure present only once in full stack trace.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private void assertLinksAppearOnce(Throwable e, List<String>... links) {
        Set<List<String>> seen  = new HashSet<>();

        while (e != null) {
            for (List<String> l : links)
                if (hasLinksInMessage(e, l))
                    assertTrue(seen.add(l));

            e = e.getCause();
        }
    }

    /**
     * @param e Exception
     * @param links List of links.
     * @return Whether exception has all passed links in it's message.
     */
    private boolean hasLinksInMessage(Throwable e, @Nullable Iterable<String> links) {
        if (links == null)
            return true;

        for (String link : links)
            if (!e.getMessage().contains(link))
                return false;

        return true;
    }
}
