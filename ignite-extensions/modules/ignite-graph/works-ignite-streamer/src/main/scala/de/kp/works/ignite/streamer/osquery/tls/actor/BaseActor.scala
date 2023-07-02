package de.kp.works.ignite.streamer.osquery.tls.actor
/*
 * Copyright (c) 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */
import akka.actor.SupervisorStrategy._
import akka.actor.{Actor, ActorLogging, ActorSystem, OneForOneStrategy}
import akka.http.scaladsl.coding.{Gzip, NoCoding}
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.HttpEncodings
import akka.stream.ActorMaterializer
import akka.util.{ByteString, Timeout}
import com.google.gson._
import com.typesafe.config.Config
import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.streamer.osquery.OsqueryConstants
import de.kp.works.ignite.streamer.osquery.tls.db.{DBApi, OsqueryNode}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.Try

abstract class BaseActor(api:DBApi) extends Actor with ActorLogging {

  import BaseActor._
  /**
   * The actor system is implicitly accompanied by a materializer,
   * and this materializer is required to retrieve the ByteString
   */
  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val osqueryCfg: Config = WorksConf.getCfg(WorksConf.OSQUERY_CONF)
  val actorCfg: Config = osqueryCfg.getConfig("actor")

  implicit val timeout: Timeout = {
    val value = actorCfg.getInt("timeout")
    Timeout(value.seconds)
  }
  /**
   * Parameters to control the handling of failed child actors:
   * it is the number of retries within a certain time window.
   *
   * The supervisor strategy restarts a child up to 10 restarts
   * per minute. The child actor is stopped if the restart count
   * exceeds maxNrOfRetries during the withinTimeRange duration.
   */
  protected val maxRetries: Int = actorCfg.getInt("maxRetries")
  protected val timeRange: FiniteDuration = {
    val value = actorCfg.getInt("timeRange")
    value.minute
  }
  /**
   * Child actors are defined leveraging a RoundRobin pool with a
   * dynamic resizer. The boundaries of the resizer are defined
   * below
   */
  protected val lower: Int = actorCfg.getInt("lower")
  protected val upper: Int = actorCfg.getInt("upper")
  /**
   * The number of instances for the RoundRobin pool
   */
  protected val instances: Int = actorCfg.getInt("instances")
  /**
   * Each actor is the supervisor of its children, and as such each
   * actor defines fault handling supervisor strategy. This strategy
   * cannot be changed afterwards as it is an integral part of the
   * actor system’s structure.
   */
  override val supervisorStrategy: OneForOneStrategy =
   /*
    * The implemented supervisor strategy treats each child separately
    * (one-for-one). Alternatives are, e.g. all-for-one.
    *
    */
    OneForOneStrategy(maxNrOfRetries = maxRetries, withinTimeRange = timeRange) {
      case _: ArithmeticException      => Resume
      case _: NullPointerException     => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception                => Escalate
    }

  override def receive: Receive = {

    case request:HttpRequest =>
      sender ! Response(Try({
        execute(request)
      })
      .recover {
        case t:Throwable =>
          /*
           * Send invalid message as response
           */
          log.error(t.getLocalizedMessage)
          throw new Exception(buildInvalidResponse)
      })

  }

  def execute(request:HttpRequest):String

  def buildInvalidResponse:String = {
    buildResponse(nodeInvalid = true)
  }

  def buildResponse(nodeInvalid:Boolean, nodeKey:Option[String] = None):String = {

    val response = new JsonObject()
    response.addProperty(OsqueryConstants.NODE_INVALID, nodeInvalid)

    if (nodeKey.isDefined) response.addProperty(OsqueryConstants.NODE_KEY, nodeKey.get)
    response.toString

  }

  /**
   * In case of `node = null` a `node_invalid`
   * response is sent to the osquery agent
   */
  def getNode(request:HttpRequest):OsqueryNode = {

    val json = getBodyAsJson(request)
    if (json == null) {
      log.error("Request did not contain valid JSON.")
      return null
    }

    val payload = json.getAsJsonObject
    val nodeKey = try {
      payload.get(OsqueryConstants.NODE_KEY).getAsString

    } catch {
      case _:Throwable => null
    }

    if (nodeKey == null) {

      log.error("Request does not contain valid node key.")
      return null

    }

    var node = api.nodeByKey(nodeKey)
    /*
     * Check whether the retrieved node is active
     */
    if (!node.active) {

      log.error("Request refers to an inactive node.")
      return null

    }

    /*
     * The remote (client) address is also registered with
     * the respective node `lastAddress` field
     */
    val remoteAddress = {

      val addressHeader = request.headers.find(_.name == "Remote-Address")
      if (addressHeader.isDefined) addressHeader.get.value() else null

    }

    if (remoteAddress != null && remoteAddress.nonEmpty)
      node = node.copy(lastAddress = remoteAddress, lastCheckIn = System.currentTimeMillis)

    node

  }

  def getBodyAsJson(request: HttpRequest):JsonElement = {

    try {

      /* BODY */

      /*
	     * Check whether the `Content-Encoding` header is set;
       * this is the case, if --logger_tls_compress=true.
       *
       * The encoding used in this case is `gzip`; other encoding
       * are not supported
       */
      val decoder = request.encoding match {
        case HttpEncodings.gzip => Gzip
        case _ => NoCoding
      }

      val decoded = decoder.decodeMessage(request)
      val source = decoded.entity.dataBytes

      /* Extract body as String from request entity */
      val future = source.runFold(ByteString(""))(_ ++ _)
      /*
       * We do not expect to retrieve large messages
       * and accept a blocking wait
       */
      val bytes = Await.result(future, timeout.duration)

      val body = bytes.decodeString("UTF-8")
      JsonParser.parseString(body)

    } catch {
      case _:Throwable => null
    }

  }
}


object BaseActor {

  case class Response(status: Try[_])

}