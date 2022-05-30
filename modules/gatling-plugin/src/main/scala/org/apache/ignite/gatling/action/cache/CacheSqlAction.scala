package org.apache.ignite.gatling.action.cache

import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.validation.{Failure, SuccessWrapper}
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.check.Check
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.gatling.SqlCheck
import org.apache.ignite.gatling.action.ActionBase
import org.apache.ignite.gatling.api.IgniteApi

import java.util
import scala.jdk.CollectionConverters._
import java.util.{HashMap => JHashMap}

case class CacheSqlAction[K, V](requestName: Expression[String],
                                cacheName: Expression[String],
                                sql: Expression[String],
                                args: List[Expression[Any]],
                                partitions: Expression[List[Int]],
                                checks: Seq[SqlCheck],
                                next: Action,
                                ctx: ScenarioContext
                               ) extends ChainableAction with NameGen with ActionBase with StrictLogging {

  override val name: String = genName("sql")

  private def resolveArgs(session: Session) =
    args
      .foldLeft(List[Any]().success) { case (r, v) =>
        r.flatMap(m => v(session).map(rv => rv :: m))
      }
      .map(l => l.reverse)

  override protected def execute(session: Session): Unit = {
    logger.debug(s"session user id: #${session.userId}, sql")

    val client: IgniteApi = session("igniteApi").as[IgniteApi]

    (for {
      resolvedRequestName <- requestName(session)
      resolvedCacheName <- cacheName(session)
      resolvedSql <- sql(session)
      resolvedArgs <- resolveArgs(session)
      resolvedPartitions <- partitions(session)
      startTime <- ctx.coreComponents.clock.nowMillis.success
    } yield {
      client.cache[K, V](resolvedCacheName)
        .map(
          cache => {
            logger.debug(s"session user id: #${session.userId}, before sql")

            val query: SqlFieldsQuery = new SqlFieldsQuery(resolvedSql)

            if (resolvedArgs.nonEmpty)
              query.setArgs(resolvedArgs: _*)
            if (resolvedPartitions.nonEmpty)
              query.setPartitions(resolvedPartitions: _*)
            query.setSchema("PUBLIC")
            cache.sql(query)(
              value => {
                logger.debug(s"session user id: #${session.userId}, after cache.sql")
                val finishTime          = ctx.coreComponents.clock.nowMillis
                val (newSession, error) = Check.check(value, session, checks.toList, new JHashMap[Any, Any]())
                error match {
                  case Some(Failure(errorMessage)) =>
                    logAndExecuteNext(newSession.markAsFailed, resolvedRequestName, startTime,
                      finishTime, KO, next, Some("Check ERROR"), Some(errorMessage))
                  case _ => logAndExecuteNext(newSession, resolvedRequestName, startTime,
                    finishTime, OK, next, None, None)
                }
              },
              ex => {
                logger.error("Error during sql query", ex)
                logAndExecuteNext(session, resolvedRequestName, startTime,
                  ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage))
              },
            )
          })
        .fold(
          ex => {
            logger.debug(s"session user id: #${session.userId}, can not get cache in get", ex)
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
