/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.visor.commands.cache

import java.util.{Calendar, Date, GregorianCalendar, UUID}

import org.apache.ignite.internal.util.lang.{GridFunc => F}
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.internal.visor.cache._
import org.apache.ignite.internal.visor.util.VisorTaskUtils._
import org.apache.ignite.visor.commands.cache.VisorCacheModifyCommand._
import org.apache.ignite.visor.commands.common.VisorConsoleCommand
import org.apache.ignite.visor.visor._

/**
 * ==Overview==
 * Visor 'modify' command implementation.
 *
 * ==Help==
 * {{{
 * +-----------------------------------------------------------------------------------------+
 * | modify -put    | Put custom value into cache.                                           |
 * +-----------------------------------------------------------------------------------------+
 * | modify -get    | Get value with specified key from cache.                               |
 * +-----------------------------------------------------------------------------------------+
 * | modify -remove | Remove value with specified key from cache.                            |
 * +-----------------------------------------------------------------------------------------+
 *
 * }}}
 *
 * ====Specification====
 * {{{
 *     modify -put -c=<cache-name> {-kt=<key-type>} {-kv=<key>} {-vt=<value-type>} {-v=<value>}
 *     modify -get -c=<cache-name> {-kt=<key-type>} {-kv=<key>}
 *     modify -remove -c=<cache-name> {-kt=<key-type>} {-kv=<key>}
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -c=<cache-name>
 *         Name of the cache.
 *     -kt=<key-type>
 *         Type of key. Default value is java.lang.String. Short type name can be specified.
 *     -kv=<key>
 *         Key. Asked in interactive mode when it is not specified.
 *     -vt=<value-type>.
 *         Type of value. Default value is java.lang.String. Short type name can be specified.
 *         Value type is equals to key type when value is not specified.
 *     -v=<value>
 *         Value. Equals to key when it is not specified.
 *         Asked in interactive mode when key and value are not specified.
 * }}}
 *
 * ====Examples====
 * {{{
 *     modify -put -c=@c0
 *         Put value into cache in interactive mode.
 *     modify -get -c=@c0
 *         Get value from cache in interactive mode.
 *     modify -remove -c=@c0
 *         Remove value from cache in interactive mode.
 *     modify -put -c=cache -kv=key1
 *         Put value into cache with name cache with key of default String type equal to key1
 *         and value equal to key.
 *     modify -put -c=cache -kt=java.lang.String -kv=key1 -vt=lava.lang.String -v=value1
 *         Put value into cache with name cache with key of String type equal to key1
 *         and value of String type equal to value1
 *     modify -get -c=cache -kt=java.lang.String -kv=key1
 *         Get value from cache with name cache with key of String type equal to key1
 *     modify -remove -c=cache -kt=java.lang.String -kv=key1
 *         Remove value from cache with name cache with key of String type equal to key1.
 *
 * }}}
 */
class VisorCacheModifyCommand extends VisorConsoleCommand {
    @impl protected val name = "modify"

    /**
     * ===Command===
     * Modify cache value in specified cache.
     *
     * ===Examples===
     * <ex>modify -put -c=@c0</ex>
     *     Put value into cache with name taken from 'c0' memory variable in interactive mode.
     * <br>
     * <ex>modify -get</ex>
     *     Get value from cache with name taken from 'c0' memory variable in interactive mode.
     * <br>
     * <ex>modify -remove</ex>
     *     Remove value from cache with name taken from 'c0' memory variable in interactive mode.
     * <br>
     * <ex>modify -put -c=cache -kt=java.lang.String -k=key1 -vt=lava.lang.String -v=value1</ex>
     *     Put value into cache with name 'cache' with key of String type equal to 'key1'
     *     and value of String type equal to 'value1'
     * <br>
     * <ex>modify -get -c=cache -kt=java.lang.String -k=key1</ex>
     *     Get value from cache with name 'cache' with key of String type equal to 'key1'
     * <br>
     * <ex>modify -remove -c=cache -kt=java.lang.String -k=key1</ex>
     *     Remove value from cache with name 'cache' with key of String type equal to 'key1'.
     *
     * @param args Command arguments.
     */
    def modify(args: String) {
        if (checkConnected() && checkActiveState()) {
            def argNonEmpty(argLst: ArgList, arg: Option[String], key: String): Boolean = {
                if (hasArgName(key, argLst) && arg.forall((a) => F.isEmpty(a))) {
                    warn(s"Argument $key is specified and can not be empty")

                    false
                }
                else
                    true
            }

            var argLst = parseArgs(args)

            val put = hasArgFlag("put", argLst)
            val get = hasArgFlag("get", argLst)
            val remove = hasArgFlag("remove", argLst)

            if (!put && !get && !remove) {
                warn("Put, get, or remove operation should be specified")

                return
            }

            if (put && get || get && remove || get && remove) {
                warn("Only one operation put, get or remove allowed in one command invocation")

                return
            }

            if (!hasArgName("c", argLst)) {
                warn("Cache name should be specified")

                return
            }

            val cacheName = argValue("c", argLst) match {
                case Some(dfltName) if dfltName == DFLT_CACHE_KEY || dfltName == DFLT_CACHE_NAME =>
                    argLst = argLst.filter(_._1 != "c") ++ Seq("c" -> null)

                    Some(null)

                case cn => cn
            }

            if (cacheName.isEmpty) {
                warn("Cache with specified name is not found")

                return
            }

            val keyTypeStr = argValue("kt", argLst)
            val keyStr = argValue("k", argLst)
            var key: Object = null

            if (keyTypeStr.nonEmpty && keyStr.isEmpty) {
                warn("Key should be specified when key type is specified")

                return
            }

            val valueTypeStr = argValue("vt", argLst)
            val valueStr = argValue("v", argLst)
            var value: Object = null

            if (valueTypeStr.nonEmpty && valueStr.isEmpty) {
                warn("Value should be specified when value type is specified")

                return
            }

            if (!argNonEmpty(argLst, keyTypeStr, "kt")
                || !argNonEmpty(argLst, keyStr, "k")
                || !argNonEmpty(argLst, valueTypeStr, "vt")
                || !argNonEmpty(argLst, valueStr, "v"))
                return

            keyTypeStr match {
                case Some(clsStr) =>
                    try {
                        INPUT_TYPES.find(_._3.getName.indexOf(clsStr) >= 0) match {
                            case Some(t) => key = t._2(keyStr.get)
                            case None =>
                                warn("Specified type is not allowed")

                                return
                        }
                    }
                    catch {
                        case e: Throwable =>
                            warn("Failed to read key: " + e.getMessage)

                            return
                    }

                case None if keyStr.nonEmpty =>
                    key = keyStr.get

                case None if put && valueStr.nonEmpty => // No-op.

                case None =>
                    askTypedValue("key") match {
                        case Some(k) if k.toString.nonEmpty => key = k
                        case _ =>
                            warn("Key can not be empty.")

                            return
                    }
            }

            if (put) {
                valueTypeStr match {
                    case Some(clsStr) =>
                        try {
                            INPUT_TYPES.find(_._3.getName.indexOf(clsStr) >= 0) match {
                                case Some(t) => value = t._2(valueStr.get)
                                case None => warn("Specified type is not allowed")

                                    return
                            }
                        }
                        catch {
                            case e: Throwable =>
                                warn("Failed to read value: " + e.getMessage)

                                return
                        }
                    case None if valueStr.nonEmpty =>
                        value = valueStr.get

                    case None =>
                        askTypedValue("value") match {
                            case Some(v) if v.toString.nonEmpty => value = v
                            case _ =>
                                warn("Value can not be empty.")

                                return
                        }
                }

                if (key == null)
                    key = value
            }

            if ((get || remove) && valueTypeStr.nonEmpty)
                warn("Specified value is not used by selected operation and will be ignored")

            val arg = new VisorCacheModifyTaskArg(cacheName.get,
                if (put) VisorModifyCacheMode.PUT else if (get) VisorModifyCacheMode.GET else VisorModifyCacheMode.REMOVE,
                key, value
            )

            try {
                val taskResult = executeRandom(classOf[VisorCacheModifyTask], arg)
                val resultObj = taskResult.getResult match {
                    case d: Date =>
                        val cal = new GregorianCalendar()
                        cal.setTime(d)

                        if (cal.get(Calendar.HOUR_OF_DAY) == 0 && cal.get(Calendar.MINUTE) == 0
                            && cal.get(Calendar.SECOND) == 0)
                            formatDate(d)
                        else
                            formatDateTime(d)

                    case v => v
                }
                val affinityNode = taskResult.getAffinityNode

                if (put) {
                    println("Put operation success" + "; Affinity node: " + nid8(affinityNode))

                    if (resultObj != null)
                        println("Previous value is: " + resultObj)
                }

                if (get) {
                    if (resultObj != null)
                        println("Value with specified key: " + resultObj + "; Affinity node: " + nid8(affinityNode))
                    else
                        println("Value with specified key not found")
                }

                if (remove) {
                    if (resultObj != null)
                        println("Removed value: " + resultObj + "; Affinity node: " + nid8(affinityNode))
                    else
                        println("Value with specified key not found")
                }
            }
            catch {
                case e: Throwable =>
                    warn("Failed to execute cache modify operation: " + e.getMessage)
            }
        }
    }

    /**
     * ===Command===
     * Modify cache data by execution of put/get/remove command.
     *
     * ===Examples===
     * <ex>modify -put -c=@c0</ex>
     * Put entity in cache with name taken from 'c0' memory variable in interactive mode
     */
    def modify() {
        this.modify("")
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorCacheModifyCommand {
    /** Singleton command */
    private val cmd = new VisorCacheModifyCommand

    /** Default cache name to show on screen. */
    private final val DFLT_CACHE_NAME = escapeName(null)

    /** Default cache key. */
    protected val DFLT_CACHE_KEY: String = DFLT_CACHE_NAME + "-" + UUID.randomUUID().toString

    addHelp(
        name = "modify",
        shortInfo = "Modify cache by put/get/remove value.",
        longInfo = Seq(
            "Execute modification of cache data:",
            " ",
            "Put new value into cache.",
            " ",
            "Get value from cache.",
            " ",
            "Remove value from cache."
        ),
        spec = Seq(
            "modify -put -c=<cache-name> {-kt=<key-type>} {-k=<key>} {-vt=<value-type>} {-v=<value>}",
            "modify -get -c=<cache-name> {-kt=<key-type>} {-k=<key>}",
            "modify -remove -c=<cache-name> {-kt=<key-type>} {-k=<key>}"
    ),
        args = Seq(
            "-c=<cache-name>" ->
                "Name of the cache",
            "-put" -> Seq(
                "Put value into cache and show its affinity node.",
                "If the cache previously contained a mapping for the key, the old value is shown",
                "Key and value are asked in interactive mode when they are not specified.",
                "Key is equals to value when key is not specified."
            ),
            "-get" -> Seq(
                "Get value from cache and show its affinity node.",
                "Key is asked in interactive mode when it is not specified."
            ),
            "-remove" -> Seq(
                "Remove value from cache and show its affinity node.",
                "Key is asked in interactive mode when it is not specified."
            ),
            "-kt=<key-type>" ->
                "Type of key. Default type is java.lang.String. Type name can be specified without package.",
            "-k=<key>" ->
                "Key. Must be specified when key type is specified.",
            "-vt=<value-type>" ->
                "Type of value. Default type is java.lang.String. Type name can be specified without package.",
            "-v=<value>" ->
                "Value. Must be specified when value type is specified."
        ),
        examples = Seq(
            "modify -put -c=@c0" ->
                "Put value into cache with name taken from 'c0' memory variable in interactive mode.",
            "modify -get -c=@c0" ->
                "Get value from cache with name taken from 'c0' memory variable in interactive mode.",
            "modify -remove -c=@c0" ->
                "Remove value from cache with name taken from 'c0' memory variable in interactive mode.",
            "modify -put -c=cache -v=value1" -> Seq(
                "Put the value 'value1' into the cache 'cache'.",
                "Other params have default values: -kt = java.lang.String , -k = value1, -vt = java.lang.String"
            ),
            "modify -put -c=@c0 -kt=java.lang.String -k=key1 -vt=lava.lang.String -v=value1" -> Seq(
                "Put value into cache with name taken from 'c0' memory variable",
                "with key of String type equal to 'key1' and value of String type equal to 'value1'"
            ),
            "modify -get -c=@c0 -kt=java.lang.String -k=key1" ->
                "Get value from cache with name taken from 'c0' memory variable with key of String type equal to key1",
            "modify -remove -c=@c0 -kt=java.lang.String -k=key1" ->
                "Remove value from cache with name taken from 'c0' memory variable with key of String type equal to key1."
        ),
        emptyArgs = cmd.modify,
        withArgs = cmd.modify
    )

    /**
     * Singleton.
     */
    def apply(): VisorCacheModifyCommand = cmd
}
