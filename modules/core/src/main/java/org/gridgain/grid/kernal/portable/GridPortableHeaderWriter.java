/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.portable.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

import static java.nio.charset.StandardCharsets.*;

/**
 * Header writer.
 */
class GridPortableHeaderWriter {
    /** */
    private static final GridPortablePrimitivesWriter PRIM = GridPortablePrimitivesWriter.get();

    /** */
    private final Collection<String> allNames = new HashSet<>();

    /** */
    private final Collection<Integer> hashCodes = new LinkedHashSet<>();

    /** */
    private Collection<byte[]> names;

    /** */
    private int hdrLen = 4;

    /**
     * @param name Field name.
     * @return Offset position.
     * @throws GridPortableException In case of error.
     */
    int addField(String name) throws GridPortableException {
        if (!allNames.add(name))
            throw new GridPortableException("Duplicate field name: " + name);

        if (hashCodes.add(name.hashCode()))
            hdrLen += 4;
        else {
            byte[] nameArr = name.getBytes(UTF_8);

            if (names == null)
                names = new ArrayList<>();

            names.add(nameArr);

            hdrLen += nameArr.length;
        }

        int offPos = hdrLen;

        hdrLen += 4;

        return offPos;
    }

    /**
     * @return Header.
     */
    byte[] header() {
        byte[] hdr = new byte[hdrLen];

        int off = 0;

        PRIM.writeInt(hdr, off, hashCodes.size());

        off += 4;

        for (Integer hashCode : hashCodes) {
            PRIM.writeInt(hdr, off, hashCode);

            off += 8;
        }

        PRIM.writeInt(hdr, off, names != null ? names.size() : -1);

        off += 4;

        if (names != null) {
            for (byte[] name : names) {
                U.arrayCopy(name, 0, hdr, off, name.length);

                off += name.length + 4;
            }
        }

        return hdr;
    }
}
