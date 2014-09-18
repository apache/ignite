/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.tools.ant.beautifier;

/**
 * Lexical token type.
 */
enum GridJavadocTokenType {
    /** HTML instruction.  */
    TOKEN_INSTR,

    /** HTML comment. */
    TOKEN_COMM,

    /** HTML open tag. */
    TOKEN_OPEN_TAG,

    /** HTML close tag. */
    TOKEN_CLOSE_TAG,

    /** HTML text. */
    TOKEN_TEXT
}
