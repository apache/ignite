// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import java.io.*;
import java.util.*;

/**
 * Portable object metadata to show in Visor.
 *
 * @author @java.author
 * @version @java.version
 */
public class VisorPortableMetadata implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Type name */
    private String typeName;

    /** Type ID */
    private Integer typeID;

    /** Filed list */
    private List<VisorPortableMetadataField> fields;

    public String typeName() {
        return typeName;
    }

    public void typeName(String typeName) {
        this.typeName = typeName;
    }

    public Integer typeID() {
        return typeID;
    }

    public void typeID(Integer typeID) {
        this.typeID = typeID;
    }

    public List<VisorPortableMetadataField> fields() {
        return fields;
    }

    public void fields(List<VisorPortableMetadataField> fields) {
        this.fields = fields;
    }
}
