package de.kp.works.ignite.streamer.fiware

/**
 * Copyright (c) 2019 - 2022 Dr. Krusche & Partner PartG. All rights reserved.
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

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.streamer.fiware.FiwareRoute._
import de.kp.works.ignite.streamer.fiware.actors.{EntitiesActor, EntityActor, NotifyActor}
import org.apache.ignite.Ignite

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

/**
 * The notification endpoint, the Orion Context Broker
 * sends (subscribed) notifications to.
 */
class FiwareServer {

  private var ignite:Option[Ignite] = None

  private var eventHandler:Option[FiwareEventHandler] = None
  private var server:Option[Future[Http.ServerBinding]] = None
  /**
   * Akka 2.6 provides a default materializer out of the box, i.e., for Scala
   * an implicit materializer is provided if there is an implicit ActorSystem
   * available. This avoids leaking materializers and simplifies most stream
   * use cases somewhat.
   */
  private val systemName = WorksConf.getSystemName(WorksConf.FIWARE_CONF)
  implicit val system: ActorSystem = ActorSystem(systemName)

  implicit lazy val context: ExecutionContextExecutor = system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  /**
   * Common timeout for all Akka connection
   */
  implicit val timeout: Timeout = Timeout(5.seconds)
  /**
   * Specify the event handler to be used by this server
   * to send Orion notification to the respective
   * Ignite cache.
   *
   * The current implementation leverages the Fiware
   * Streamer as callback
   */
  def setEventHandler(handler:FiwareEventHandler):FiwareServer = {
    this.eventHandler = Some(handler)
    this
  }

  def setIgnite(ignite:Ignite):FiwareServer = {
    this.ignite = Some(ignite)
    this
  }
  /**
   * This method launches the Orion notification server and also subscribes
   * to the Orion Context Broker for receiving NGSI event notifications
   */
  def launch():Unit = {
    if (eventHandler.isEmpty)
      throw new Exception("[FiwareServer] No callback specified to send notifications to.")

    /*
     * The [EntityActor] is used to retrieve a certain
     * NGSI entity and its registered telemetry data
     */
    val entityActor = system
      .actorOf(Props(new EntityActor(ignite.get)),ENTITY_ACTOR)
    /*
     * The [EntitiesActor] is used to retrieved (all)
     * registered NGSI entities
     */
    val entitiesActor = system
      .actorOf(Props(new EntitiesActor(ignite.get)),ENTITIES_ACTOR)

    /*
     * The [NotifyActor] is used to receive NGSI events
     * and delegate them to the provided callback
     */
    val notifyActor = system
      .actorOf(Props(new NotifyActor(eventHandler.get)), NOTIFY_ACTOR)

    val actors = Map(
      NOTIFY_ACTOR   -> notifyActor,
      ENTITY_ACTOR   -> entityActor,
      ENTITIES_ACTOR -> entitiesActor)

    val fiwareRoute = new FiwareRoute(actors)
    val routes:Route = fiwareRoute.buildRoute
    /**
     * The host & port configuration of the HTTP server that
     * is used as a notification endpoint for an Orion Context
     * Broker instance
     */
    val serverCfg = WorksConf.getServerCfg(WorksConf.FIWARE_CONF)
    val bindingCfg = serverCfg.getConfig("binding")

    val (host, port) = (bindingCfg.getString("host"), bindingCfg.getInt("port"))
    /*
     * Distinguish between SSL/TLS and non-SSL/TLS requests
     */
    server = if (FiwareSsl.isServerSsl) {
       /*
        * The request protocol in the notification url must be
        * specified as 'http://'
        */
       Some(Http().bindAndHandle(routes , host, port))

    } else {
      /*
       * The request protocol in the notification url must
       * be specified as 'https://'. In this case, an SSL
       * security context must be specified
       */
      val context = FiwareSsl.buildServerContext
      Some(Http().bindAndHandle(routes, host, port, connectionContext = context))

    }

    /* STEP #3: Register subscriptions with Fiware Context Broker */

    val subscriptions = FiwareSubscriptions.getSubscriptions
    subscriptions.foreach(subscription => {

      try {
        /*
         * The current implementation does not support
         * `service` and `servicepath` specification
         */
        val service:Option[String] = None
        val servicePath:Option[String] = None

        val future = FiwareClient.subscribe(subscription, service, servicePath, system)
        val response = Await.result(future, timeout.duration)

        val sid = FiwareClient.getSubscriptionId(response)
        FiwareSubscriptions.register(sid, subscription)

      } catch {
        case _:Throwable =>
          /*
           * The current implementation of the Fiware streaming
           * support is an optimistic approach that focuses on
           * those subscriptions that were successfully registered.
           */
          println("[ERROR] ------------------------------------------------")
          println("[ERROR] Registration of subscription failed:")
          println(s"$subscription")
          println("[ERROR] ------------------------------------------------")
      }

    })

  }

  def stop():Unit = {

    if (server.isEmpty)
      throw new Exception("Fiware server was not launched.")

    server.get
      /*
       * rigger unbinding from port
       */
      .flatMap(_.unbind())
      /*
       * Shut down application
       */
      .onComplete(_ => {
        system.terminate()
      })

  }
}
