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
package org.apache.ignite.gatling.action

import java.util.{HashMap => JHashMap}

import io.gatling.commons.stats.KO
import io.gatling.commons.stats.OK
import io.gatling.commons.stats.Status
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.Failure
import io.gatling.commons.validation.Success
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.commons.validation.Validation
import io.gatling.core.action.Action
import io.gatling.core.action.ChainableAction
import io.gatling.core.check.Check
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.api.TransactionApi
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.gatling.protocol.IgniteProtocol.IgniteApiSessionKey
import org.apache.ignite.gatling.protocol.IgniteProtocol.TransactionApiSessionKey

/**
 * Base class for all Ignite actions.
 *
 * @param actionType Action type name.
 * @param requestName Name of the request provided via the DSL. May be empty.  If so the defaultRequestName will be used.
 * @param ctx Gatling scenario context.
 * @param next Next action to execute in scenario chain.
 */
abstract class IgniteAction(val actionType: String, val requestName: Expression[String], val ctx: ScenarioContext, val next: Action)
    extends ChainableAction
    with NameGen {

  /** @return Default request name if none was provided via the DSL. */
  def defaultRequestName: Expression[String] = _ => name.success
  /** @return Action name. */
  val name: String = genName(actionType)

  /** Clock used to measure time the action takes. */
  val clock: Clock = ctx.protocolComponentsRegistry.components(IgniteProtocol.IgniteProtocolKey).coreComponents.clock
  /** Statistics engine */
  val statsEngine: StatsEngine = ctx.protocolComponentsRegistry.components(IgniteProtocol.IgniteProtocolKey).coreComponents.statsEngine
  /** Ignite protocol */
  val protocol: IgniteProtocol = ctx.protocolComponentsRegistry.components(IgniteProtocol.IgniteProtocolKey).igniteProtocol

  /**
   * Logs results of action execution and starts the next action in the chain.
   *
   * @param session Session.
   * @param requestName Name of the request.
   * @param sent Timestamp of the request processing start.
   * @param received Timestamp of the request processing finish.
   * @param status Status of the executed request.
   * @param responseCode Optional string representation of response code in case action failed.
   * @param message Optional error message in case action failed.
   */
  protected def logAndExecuteNext(
    session: Session,
    requestName: String,
    sent: Long,
    received: Long,
    status: Status,
    responseCode: Option[String],
    message: Option[String]
  ): Unit = {
    statsEngine.logResponse(
      session.scenario,
      session.groups,
      requestName,
      sent,
      received,
      status,
      responseCode,
      message
    )
    next ! session.logGroupRequestTimings(sent, received)
  }

  /**
   * Executes an action in context of Session. Logs crash if session context is not valid.
   *
   * @param session Session.
   * @param f Function executing an action.
   */
  protected def withSessionCheck(session: Session)(f: => Validation[Unit]): Unit = {
    logger.debug(s"session user id: #${session.userId}, $name")
    f
      .onFailure { ex =>
        val resolvedRequestName =
          requestName(session).toOption.filter(_.nonEmpty).getOrElse(defaultRequestName(session).toOption.getOrElse(name))
        logger.error(
          s"Exception in ignite action during session check, user id: #${session.userId}, requesst name: $resolvedRequestName",
          ex
        )

        statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, ex)
        statsEngine.logResponse(
          session.scenario,
          session.groups,
          resolvedRequestName,
          0,
          0,
          KO,
          Some("CRASH"),
          Some(ex)
        )
        next ! session.markAsFailed
      }
  }

  /**
   * Common parameters for ignite actions.
   *
   * @param requestName Name of request.
   * @param igniteApi Instance of IgniteApi.
   * @param transactionApi Instance of TransactionApi. Present in session context if Ignite transaction was started.
   */
  case class IgniteActionParameters(requestName: String, igniteApi: IgniteApi, transactionApi: Option[TransactionApi])

  /**
   * Resolves the request name.
   *
   * If name is resolved successfully to empty string (it may be either it wasn't specified via DSL
   * or explicitly configured empty) the default one is used.
   *
   * @param session Session
   * @return Some non-empty request name.
   */
  def resolveRequestName(session: Session): Validation[String] =
    requestName(session) match {
      case f: Failure     => f
      case Success(value) => if (value.isEmpty) defaultRequestName(session) else value.success
    }

  /**
   * Resolves ignite action parameters using session context.
   *
   * @param session Session.
   * @return Instance of IgniteActionParameters.
   */
  def resolveIgniteParameters(session: Session): Validation[IgniteActionParameters] =
    for {
      resolvedRequestName <- resolveRequestName(session)
      igniteApi <- session(IgniteApiSessionKey).validate[IgniteApi]
      transactionApi <- session(TransactionApiSessionKey).asOption[TransactionApi].success
    } yield IgniteActionParameters(resolvedRequestName, igniteApi, transactionApi)

  /**
   * Helper no-op placeholder for session update lambda.
   *
   * @tparam R Result type.
   * @return Session.
   */
  private def keepSessionUnchanged[R]: (Session, Option[R]) => Session = (s: Session, _: Option[R]) => s

  /**
   * Calls function from the Ignite API,
   * performs checks against the result,
   * records outcome and execution time and
   * invokes next action in the chain.
   *
   * In case of successful execution of function calls the lambda provided to update the session state.
   *
   * @tparam R Type of the result.
   * @param func Function to execute (some function from the Ignite API).
   * @param resolvedRequestName Resolved request name.
   * @param session Session.
   * @param checks List of checks to perform (may be empty).
   * @param updateSession Function to be called to update session on successful function execution. Keep
   *                      session unchanged if absent.
   */
  def call[R](
    func: (R => Unit, Throwable => Unit) => Unit,
    resolvedRequestName: String,
    session: Session,
    checks: Seq[Check[R]] = Seq.empty,
    updateSession: (Session, Option[R]) => Session = keepSessionUnchanged
  ): Unit = {

    val startTime = clock.nowMillis
    func(
      result => {
        logger.debug(s"session user id: #${session.userId}, after $name")
        val finishTime = clock.nowMillis
        val (checkedSession, error) = Check.check(result, session, checks.toList, new JHashMap[Any, Any]())

        error match {
          case Some(Failure(errorMessage)) =>
            logAndExecuteNext(
              updateSession(checkedSession.markAsFailed, Some(result)),
              resolvedRequestName,
              startTime,
              finishTime,
              KO,
              Some("Check ERROR"),
              Some(errorMessage)
            )
          case None =>
            logAndExecuteNext(updateSession(checkedSession, Some(result)), resolvedRequestName, startTime, finishTime, OK, None, None)
        }
      },
      ex => {
        logger.error(s"Exception in ignite actions, user id: #${session.userId}, requesst name: $resolvedRequestName", ex)

        session(TransactionApiSessionKey).asOption[TransactionApi].foreach(_.close()(_ => (), _ => ()))
        logAndExecuteNext(
          session.markAsFailed,
          resolvedRequestName,
          startTime,
          clock.nowMillis,
          KO,
          Some("ERROR"),
          Some(ex.getMessage)
        )
      }
    )
  }
}
