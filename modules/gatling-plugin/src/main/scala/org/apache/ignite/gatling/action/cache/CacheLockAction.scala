package org.apache.ignite.gatling.action.cache

import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.validation.{Failure, SuccessWrapper}
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.check.Check
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.IgniteCheck
import org.apache.ignite.gatling.action.ActionBase
import org.apache.ignite.gatling.api.IgniteApi

import java.util.concurrent.locks.Lock
import java.util.{HashMap => JHashMap}

case class CacheLockAction[K, V](requestName: Expression[String],
                                 cacheName: Expression[String],
                                 key: Expression[K],
                                 checks: Seq[IgniteCheck[K, Lock]],
                                 next: Action,
                                 ctx: ScenarioContext
                               ) extends ChainableAction with NameGen with ActionBase with StrictLogging {

  override val name: String = genName("cacheLock")

  override protected def execute(session: Session): Unit = {
    logger.debug(s"session user id: #${session.userId}, lock")

    val client: IgniteApi = session("igniteApi").as[IgniteApi]

    (for {
      resolvedRequestName <- requestName(session)//                logAndExecuteNext(session.set("lock", value), resolvedRequestName, startTime,
//                  finishTime, OK, next, None, None)

      resolvedCacheName <- cacheName(session)
      resolvedKey <- key(session)
      startTime <- ctx.coreComponents.clock.nowMillis.success
    } yield {
      client.cache[K, V](resolvedCacheName)
        .map(
          cache => {
            logger.debug(s"session user id: #${session.userId}, before cache.lock")

            val lockCall = cache.lock(resolvedKey) _

            lockCall(
              value => {
                logger.debug(s"session user id: #${session.userId}, after cache.lock")
                val finishTime          = ctx.coreComponents.clock.nowMillis

                val (newSession, error) = Check.check(Map(resolvedKey -> value), session, checks.toList,
                  new JHashMap[Any, Any]())
                error match {
                  case Some(Failure(errorMessage)) =>
                    logAndExecuteNext(newSession.markAsFailed, resolvedRequestName, startTime,
                      finishTime, KO, next, Some("Check ERROR"), Some(errorMessage))
                  case _ => logAndExecuteNext(newSession, resolvedRequestName, startTime,
                    finishTime, OK, next, None, None)
                }
              },
              ex => logAndExecuteNext(session, resolvedRequestName, startTime,
                ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage)),
            )
          })
        .fold(
          ex => {
            logger.debug(s"session user id: #${session.userId}, can not get cache in lock", ex)
            logAndExecuteNext(session, resolvedRequestName, startTime,
              ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage))
          },
          _ => {}
        )
    })
      .onFailure(ex =>
        requestName(session).map { resolvedRequestName =>
          ctx.coreComponents.statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, ex)
          executeNext(session, next)
        },
      )
  }
}
