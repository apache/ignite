/* @scala.file.header */

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.pimps

/**
 * Sub class to create a wrapper type for `X` as documentation that the sub class follows the
 * 'pimp my library' pattern. http://www.artima.com/weblogs/viewpost.jsp?thread=179766
 * <p/>
 * The companion object provides an implicit conversion to unwrap `value`.
 *
 * @author @java.author
 * @version @java.version
 */
trait PimpedType[X] {
    val value: X
}

object PimpedType {
    implicit def UnwrapPimpedType[X](p: PimpedType[X]): X = p.value
}
