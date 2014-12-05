/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import java.lang.annotation.*;

/**
 * Annotates whether or not multiple instances of this SPI can be
 * started in the same VM. This annotation should be attached to SPI
 * implementation class.
 * <p>
 * <b>Note:</b> if this annotations is omitted on SPI it will be
 * assumed that SPI doesn't support multiple grid instances on the
 * same VM.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface IgniteSpiMultipleInstancesSupport {
    /**
     * Whether or not target SPI supports multiple grid instances
     * started in the same VM.
     */
    @SuppressWarnings({"JavaDoc"}) public boolean value();
}
