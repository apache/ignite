// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import java.io.*;

/**
 * Portable object metadata field information.
 *
 * @author @java.author
 * @version @java.version
 */
public class VisorPortableMetadataField implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Field name */
    private String name;

    /** Field fieldType name */
    private String fieldType;

    /** Field hash code */
    private String hash;

    public String name() {
        return name;
    }

    public void name(String name) {
        this.name = name;
    }

    public String fieldType() {
        return fieldType;
    }

    public void fieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String hash() {
        return hash;
    }

    public void hash(String hash) {
        this.hash = hash;
    }
}
