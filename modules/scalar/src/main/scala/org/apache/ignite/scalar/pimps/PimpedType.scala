/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.scalar.pimps

/**
 * Sub class to create a wrapper type for `X` as documentation that the sub class follows the
 * 'pimp my library' pattern. http://www.artima.com/weblogs/viewpost.jsp?thread=179766
 * <p/>
 * The companion object provides an implicit conversion to unwrap `value`.
 */
trait PimpedType[X] {
    val value: X
}

object PimpedType {
    implicit def UnwrapPimpedType[X](p: PimpedType[X]): X = p.value
}
