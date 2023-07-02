package de.kp.works.ignite.streamer.osquery.tls
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

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.streamer.osquery.tls.actor._
import de.kp.works.ignite.streamer.osquery.tls.db.DBApi

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

/**
 * The TLS endpoint, Osquery agents send query results
 * and status messages to. This Akka-based HTTP(s) server
 * also supports Osquery node configuration and management.
 */
class TLSServer(api:DBApi) {

  import TLSRoutes._

  private var eventHandler:Option[TLSEventHandler] = None
  private var server:Option[Future[Http.ServerBinding]] = None
  /**
   * Akka 2.6 provides a default materializer out of the box, i.e., for Scala
   * an implicit materializer is provided if there is an implicit ActorSystem
   * available. This avoids leaking materializers and simplifies most stream
   * use cases somewhat.
   */
  private val systemName = WorksConf.getSystemName(WorksConf.OSQUERY_CONF)
  implicit val system: ActorSystem = ActorSystem(systemName)

  implicit lazy val context: ExecutionContextExecutor = system.dispatcher

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  /**
   * Common timeout for all Akka connection
   */
  implicit val timeout: Timeout = Timeout(5.seconds)
  /**
   * Specify the event handler to be used by this server
   * to send Osquery events to the respective Ignite cache.
   *
   * The current implementation leverages the Osquery
   * Streamer as event handler
   */
  def setEventHandler(handler:TLSEventHandler):TLSServer = {
    this.eventHandler = Some(handler)
    this
  }
  /**
   * This method launches the Osquery TLS event endpoint
   */
  def launch():Unit = {

    if (eventHandler.isEmpty)
      throw new Exception("[OsqueryServer] No callback specified to send notifications to.")

    /**
     * The host & port configuration of the HTTP(s) server that
     * is used as a TLS endpoint for the Osquery agents
     */
    val serverCfg = WorksConf.getServerCfg(WorksConf.OSQUERY_CONF)
    val bindingCfg = serverCfg.getConfig("binding")

    val (host, port) = (bindingCfg.getString("host"), bindingCfg.getInt("port"))
    /*
     * Distinguish between SSL/TLS and non-SSL/TLS requests
     */
    server = if (TLSSsl.isServerSsl) {
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
      val context = TLSSsl.buildServerContext
      Some(Http().bindAndHandle(routes, host, port, connectionContext = context))

    }

  }

  private def routes: Route = {

    lazy val adminActor = system
      .actorOf(Props(new AdminActor(api)), ADMIN_ACTOR)

    /*
     * NODE MANAGEMENT
     *
     * The [OsqueryBeat] can also be used as a gate
     * to a fleet of machines that are equipped with
     * Osquery agents.
     */
    lazy val configActor = system
      .actorOf(Props(new ConfigActor(api)), CONFIG_ACTOR)

    lazy val enrollActor = system
      .actorOf(Props(new EnrollActor(api)), ENROLL_ACTOR)
    /*
     * INFORMATION MANAGEMENT
     */
    lazy val logActor = system
      .actorOf(Props(new LogActor(api, eventHandler.get)), LOG_ACTOR)

    lazy val readActor = system
      .actorOf(Props(new ReadActor(api)), READ_ACTOR)

    lazy val writeActor = system
      .actorOf(Props(new WriteActor(api, eventHandler.get)), WRITE_ACTOR)

    val actors = Map(
      ADMIN_ACTOR  -> adminActor,
      CONFIG_ACTOR -> configActor,
      ENROLL_ACTOR -> enrollActor,
      LOG_ACTOR    -> logActor,
      READ_ACTOR   -> readActor,
      WRITE_ACTOR  -> writeActor
    )

    val routes = new TLSRoutes(actors)

    routes.admin ~
    routes.config ~
    routes.enroll ~
    routes.log ~
    routes.read ~
    routes.write

  }

  def stop():Unit = {

    if (server.isEmpty)
      throw new Exception("TLS event server was not launched.")

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
