/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spark

import org.apache.ignite.Ignite
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.lang.IgniteOutClosure
import org.apache.spark.api.java.JavaSparkContext

import scala.reflect.ClassTag

/**
 * Java-friendly Ignite context wrapper.
 *
 * @param sc Java Spark context.
 * @param cfgF Configuration factory.
 * @tparam K Key type.
 * @tparam V Value type.
 */
class JavaIgniteContext[K, V](
    @transient val sc: JavaSparkContext,
    val cfgF: IgniteOutClosure[IgniteConfiguration],
    @deprecated("Embedded mode is deprecated and will be discontinued. Consider using standalone mode instead.")
    standalone: Boolean = true
    ) extends Serializable {

    @transient val ic: IgniteContext = new IgniteContext(sc.sc, () => cfgF.apply(), standalone)

    def this(sc: JavaSparkContext, cfgF: IgniteOutClosure[IgniteConfiguration]) {
        this(sc, cfgF, true)
    }

    def this(sc: JavaSparkContext, springUrl: String) {
        this(sc, new IgniteOutClosure[IgniteConfiguration] {
            override def apply() = IgnitionEx.loadConfiguration(springUrl).get1()
        })
    }

    @deprecated("Embedded mode is deprecated and will be discontinued. Consider using standalone mode instead.")
    def this(sc: JavaSparkContext, springUrl: String, standalone: Boolean) {
        this(sc, new IgniteOutClosure[IgniteConfiguration] {
            override def apply() = IgnitionEx.loadConfiguration(springUrl).get1()
        }, standalone)
    }

    def fromCache(cacheName: String): JavaIgniteRDD[K, V] =
        JavaIgniteRDD.fromIgniteRDD(new IgniteRDD[K, V](ic, cacheName, null, false))

    def fromCache(cacheCfg: CacheConfiguration[K, V]) =
        JavaIgniteRDD.fromIgniteRDD(new IgniteRDD[K, V](ic, cacheCfg.getName, cacheCfg, false))

    def ignite(): Ignite = ic.ignite()

    def close(shutdownIgniteOnWorkers:Boolean = false) = ic.close(shutdownIgniteOnWorkers)

    private[spark] def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

    implicit val ktag: ClassTag[K] = fakeClassTag

    implicit val vtag: ClassTag[V] = fakeClassTag
}
