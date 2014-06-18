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
    private static final GridPortablePrimitives PRIM = GridPortablePrimitives.get();

    /** */
    private final Collection<String> allNames = new HashSet<>();

    /** */
    private final Collection<Integer> hashCodes;

    /** */
    private final Collection<byte[]> names;

    /** */
    private final boolean useNames;

    /** */
    private int hdrLen = 4;

    /**
     * @param useNames Whether to use names instead of hash codes.
     */
    GridPortableHeaderWriter(boolean useNames) {
        this.useNames = useNames;

        hashCodes = useNames ? null : new LinkedHashSet<Integer>();
        names = useNames ? new ArrayList<byte[]>() : null;
    }

    /**
     * @param name Field name.
     * @return Offset position.
     * @throws GridPortableException In case of error.
     */
    int addField(String name) throws GridPortableException {
        if (!allNames.add(name))
            throw new GridPortableException("Duplicate field name: " + name);

        if (useNames) {
            byte[] nameArr = name.getBytes(UTF_8);

            names.add(nameArr);

            hdrLen += nameArr.length;
        }
        else {
            if (hashCodes.add(name.hashCode()))
                hdrLen += 4;
            else
                throw new GridPortableException("Hash code collision for field: " + name); // TODO: Proper message.
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

        if (useNames) {
            PRIM.writeInt(hdr, off, names.size());

            off += 4;

            for (byte[] name : names) {
                U.arrayCopy(name, 0, hdr, off, name.length);

                off += name.length + 4;
            }
        }
        else {
            PRIM.writeInt(hdr, off, hashCodes.size());

            off += 4;

            for (Integer hashCode : hashCodes) {
                PRIM.writeInt(hdr, off, hashCode);

                off += 8;
            }
        }

        return hdr;
    }
}
