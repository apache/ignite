/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene;

import org.slf4j.helpers.MessageFormatter;

/**
 * 
 * IndexException
 * 
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Manuel Núñez (manuel.nunez@hawkore.com)
 * 
 */
public class IndexException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /** Constructs a new index exception with {@code null} as its
     * detail message.  The cause is not initialized, and may subsequently be
     * initialized by a call to {@link #initCause}.
     */
    public IndexException() {
        super();
    }

    /** Constructs a new index exception with the specified detail message.
     * The cause is not initialized, and may subsequently be initialized by a
     * call to {@link #initCause}.
     *
     * @param   message   the detail message. The detail message is saved for
     *          later retrieval by the {@link #getMessage()} method.
     */
    public IndexException(String message) {
        super(message);
    }

    /**
     * Constructs a new index exception with the specified detail message and
     * cause.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in
     * this IndexException's detail message.
     *
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public IndexException(String message, Throwable cause) {
        super(message, cause);
    }

    /** Constructs a new index exception with the specified cause and a
     * detail message of <tt>(cause==null ? null : cause.toString())</tt>
     * (which typically contains the class and detail message of
     * <tt>cause</tt>).  This constructor is useful for IndexExceptions
     * that are little more than wrappers for other throwables.
     *
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public IndexException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new index exception with the specified detail
     * message, cause, suppression enabled or disabled, and writable
     * stack trace enabled or disabled.
     *
     * @param  message the detail message.
     * @param cause the cause.  (A {@code null} value is permitted,
     * and indicates that the cause is nonexistent or unknown.)
     * @param enableSuppression whether or not suppression is enabled
     *                          or disabled
     * @param writableStackTrace whether or not the stack trace should
     *                           be writable
     *
     */
    protected IndexException(String message, Throwable cause,
                               boolean enableSuppression,
                               boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    /**
     * Constructs a new index exception with the specified formatted detail
     * message.
     *
     * @param cause
     *            the cause
     * @param message
     *            the detail message
     * @param a1
     *            first argument
     */
    public IndexException(String message, Object a1) {
        super(format1(message, a1), null);
    }

    /**
     * Constructs a new index exception with the specified formatted detail
     * message.
     *
     * @param cause
     *            the cause
     * @param message
     *            the detail message
     * @param a1
     *            first argument
     * @param a2
     *            second argument
     */
    public IndexException(String message, Object a1, Object a2) {
        this(format2(message, a1, a2), null);
    }

    /**
     * Constructs a new index exception with the specified formatted detail
     * message.
     *
     * @param cause
     *            the cause
     * @param message
     *            the detail message
     * @param a1
     *            first argument
     * @param a2
     *            second argument
     * @param a3
     *            third argument
     */
    public IndexException(String message, Object a1, Object a2, Object a3) {
        this(formatN(message, a1, a2, a3), null);
    }

    /**
     * Constructs a new index exception with the specified formatted detail
     * message.
     *
     * @param cause
     *            the cause
     * @param message
     *            the detail message
     * @param a1
     *            first argument
     * @param a2
     *            second argument
     * @param a3
     *            third argument
     * @param a4
     *            fourth argument
     */
    public IndexException(String message, Object a1, Object a2, Object a3, Object a4) {

        this(formatN(message, a1, a2, a3, a4), null);
    }

    /**
     * Constructs a new index exception with the specified formatted detail
     * message.
     *
     * @param cause
     *            the cause
     * @param message
     *            the detail message
     */
    public IndexException(Throwable cause, String message) {
        this(message, cause);
    }

    /**
     * Constructs a new index exception with the specified formatted detail
     * message.
     *
     * @param cause
     *            the cause
     * @param message
     *            the detail message
     * @param a1
     *            first argument
     */
    public IndexException(Throwable cause, String message, Object a1) {
        this(format1(message, a1), cause);
    }

    /**
     * Constructs a new index exception with the specified formatted detail
     * message.
     *
     * @param cause
     *            the cause
     * @param message
     *            the detail message
     * @param a1
     *            first argument
     * @param a2
     *            second argument
     */
    public IndexException(Throwable cause, String message, Object a1, Object a2) {
        this(format2(message, a1, a2), cause);
    }

    /**
     * Constructs a new index exception with the specified formatted detail
     * message.
     *
     * @param cause
     *            the cause
     * @param message
     *            the detail message
     * @param a1
     *            first argument
     * @param a2
     *            second argument
     * @param a3
     *            third argument
     */
    public IndexException(Throwable cause, String message, Object a1, Object a2, Object a3) {
        this(formatN(message, a1, a2, a3), cause);
    }

    /**
     * Constructs a new index exception with the specified formatted detail
     * message.
     *
     * @param cause
     *            the cause
     * @param message
     *            the detail message
     * @param a1
     *            first argument
     * @param a2
     *            second argument
     * @param a3
     *            third argument
     * @param a4
     *            fourth argument
     */
    public IndexException(Throwable cause, String message, Object a1, Object a2, Object a3, Object a4) {

        this(formatN(message, a1, a2, a3, a4), cause);
    }

    private static String format1(String message, Object arg) {
        return MessageFormatter.format(message, arg).getMessage();
    }

    private static String format2(String message, Object a1, Object a2) {
        return MessageFormatter.format(message, a1, a2).getMessage();

    }

    private static String formatN(String message, Object... as) {
        return MessageFormatter.arrayFormat(message, as).getMessage();

    }

}
