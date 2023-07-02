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

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.http.scaladsl.server.{Directive0, Route}

import scala.concurrent.duration.DurationInt

/**
 * From https://dzone.com/articles/handling-cors-in-akka-http
 */
trait CORS {
  /*
   * The definition of the response header provided by the
   * server, as CORS is a server side problem.
   */
  private val headers = List(
    `Access-Control-Allow-Origin`.*,
    `Access-Control-Allow-Credentials`(true),
    `Access-Control-Allow-Headers`("Accept", "Authorization", "Content-Type", "X-Requested-With"),
    /*
     * This part of the response header tells the browser
     * to cache the OPTIONS requests for 1 day
     */
    `Access-Control-Max-Age`(1.day.toMillis)
  )

  /*
   * This directive adds access control headers to normal
   * responses
   */
  private def addHeaders(): Directive0 = {
    respondWithHeaders(headers)
  }

  /*
   * This method handles pre-flight OPTIONS requests
   */
  private def preflightRequestHandler: Route = options {
    complete(
      HttpResponse(StatusCodes.OK).withHeaders(
        `Access-Control-Allow-Methods`(OPTIONS, POST, PUT, GET, DELETE)
      )
    )
  }

  /*
   * Wrap the route with this method to enable
   * adding of CORS headers
   */
  def addCors(route: Route): Route = addHeaders() {
    preflightRequestHandler ~ route
  }

  /*
   * This is a helper method to enrich a HttpResponse
   * with a CORS header
   */
  def addCorsHeaders(response: HttpResponse): HttpResponse =
    response.withHeaders(headers)
}
