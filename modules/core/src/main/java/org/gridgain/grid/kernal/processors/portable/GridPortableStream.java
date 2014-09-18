/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.portable;

/**
 * Portable stream.
 */
public interface GridPortableStream {
    /**
     * @return Position.
     */
    public int position();

    /**
     * @param pos Position.
     */
    public void position(int pos);

    /**
     * @return Underlying array.
     */
    public byte[] array();

    /**
     * @return Copy of data in the stream.
     */
    public byte[] arrayCopy();
}
