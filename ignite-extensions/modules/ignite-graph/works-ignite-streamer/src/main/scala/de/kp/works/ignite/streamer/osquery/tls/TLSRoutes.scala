package de.kp.works.ignite.streamer.osquery.tls
/*
 * Copyright (c) 2020 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import akka.actor.ActorRef
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.pattern.ask
import akka.util.{ByteString, Timeout}
import de.kp.works.ignite.streamer.osquery.tls.actor.BaseActor._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object TLSRoutes {

  val ADMIN_ACTOR = "AdminActor"

  val CONFIG_ACTOR = "ConfigActor"
  val ENROLL_ACTOR = "EnrollActor"

  val LOG_ACTOR = "LogActor"

  val READ_ACTOR  = "ReadActor"
  val WRITE_ACTOR = "WriteActor"

}

/**
 * [TLSRoutes] support the state-of-the-art communication
 * with an Osquery agent.
 */
class TLSRoutes(actors:Map[String, ActorRef]) extends CORS {
  /*
 	 * Common timeout for all Akka connections
   */
  implicit val timeout: Timeout = Timeout(5.seconds)

  import TLSRoutes._
  /*
   * Each route is accompanied by its own actor
   */
  private val adminActor = actors(ADMIN_ACTOR)

  private val configActor = actors(CONFIG_ACTOR)
  private val enrollActor = actors(ENROLL_ACTOR)

  private val logActor = actors(LOG_ACTOR)

  private val readActor = actors(READ_ACTOR)
  private val writeActor = actors(WRITE_ACTOR)

  /** ADMIN **/

  def admin:Route = {
    path("admin") {
      post {
        extractAdmin
      } ~
        put {
          extractAdmin
        }
    }  ~
      path("v1" / "admin") {
        post {
          extractAdmin
        } ~
          put {
            extractAdmin
          }
      }
  }

  private def extractAdmin = extract(adminActor)

  /** CONFIG **/

  /**
   * Retrieve an osquery configuration for a given node.
   * Returns an osquery configuration `file`
   */
  def config:Route = {
    path("config") {
      post {
        extractConfig
      } ~
        put {
          extractConfig
        }
    }  ~
      path("v1" / "config") {
        post {
          extractConfig
        } ~
          put {
            extractConfig
          }
      }
  }

  private def extractConfig = extract(configActor)

  /** ENROLL **/

  /**
   *  Enroll an endpoint with osquery. Request processing starts
   *  from a low-level HTTP(s) request.
   *
   *  Returns a `node_key` unique id. Additionally `node_invalid`
   *  will be true if the node failed to enroll.
   *
   */
  def enroll:Route = {
    path("enroll") {
      post {
        extractEnroll
      } ~
        put {
          extractEnroll
        }
    }  ~
      path("v1" / "enroll") {
        post {
          extractEnroll
        } ~
          put {
            extractEnroll
          }
      }
  }

  /*
   * The enrollment response is a JSON object with
   * keys `node_invalid` and optional `node_key`.
   */
  private def extractEnroll = extract(enrollActor)

  /** LOG **/

  def log:Route = {
    path("log") {
      post {
        extractLog
      } ~
        put {
          extractLog
        }
    }  ~
      path("v1" / "log") {
        post {
          extractLog
        } ~
          put {
            extractLog
          }
      }
  }

  private def extractLog = extract(logActor)

  /** READ **/

  def read:Route = {
    path("distributed" / "read") {
      post {
        extractRead
      } ~
        put {
          extractRead
        }
    }  ~
      path("v1" / "distributed" / "read") {
        post {
          extractRead
        } ~
          put {
            extractRead
          }
      }
  }

  private def extractRead = extract(readActor)

  /** WRITE
   *
   * This route receives the results of distributed queries
   * initiated by a distributed READ request. The results are
   * formatted and enriched and serialized for publishing.
   */
  def write:Route = {
    path("distributed" / "write") {
      post {
        extractWrite
      } ~
        put {
          extractWrite
        }
    }  ~
      path("v1" / "distributed" / "write") {
        post {
          extractWrite
        } ~
          put {
            extractWrite
          }
      }
  }

  private def extractWrite = extract(writeActor)

  private def extract(actor:ActorRef) = {
    extractRequest { request =>
      complete {
        /*
         * The Http(s) request is sent to the respective
         * actor and the actor' response is sent to the
         * requester (node or agent) as response.
         */
        val future = actor ? request
        Await.result(future, timeout.duration) match {
          case Response(Failure(e)) =>
            val message = e.getMessage
            jsonResponse(message)
          case Response(Success(answer)) =>
            val message = answer.asInstanceOf[String]
            jsonResponse(message)
        }
      }
    }
  }

  private def jsonResponse(message:String) = {

    val length = message.getBytes.length

    val headers = List(
      `Content-Type`(`application/json`),
      `Content-Length`(length)
    )

    HttpResponse(
      status=StatusCodes.OK,
      headers = headers,
      entity = ByteString(message),
      protocol = HttpProtocols.`HTTP/1.1`)

  }

}

