package org.apache.ignite.gatling.action.cache

import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.action.ActionBase
import org.apache.ignite.gatling.api.IgniteApi

case class CachePutAction[K, V](requestName: Expression[String],
                                cacheName: Expression[String],
                                key: Expression[K],
                                value: Expression[V],
                                next: Action,
                                ctx: ScenarioContext
                               ) extends ChainableAction with NameGen with ActionBase with StrictLogging {

  override val name: String = genName("cachePut")

  override protected def execute(session: Session): Unit = {
    logger.debug(s"session user id: #${session.userId}, put")
    val client: IgniteApi = session("igniteApi").as[IgniteApi]

    (for {
      resolvedRequestName <- requestName(session)
      resolvedCacheName <- cacheName(session)
      resolvedKey <- key(session)
      resolvedValue <- value(session)
      startTime <- ctx.coreComponents.clock.nowMillis.success
    } yield client
      .cache[K, V, Unit](resolvedCacheName)(
        cache => {
          logger.debug(s"session user id: #${session.userId}, before cache.put")
          cache.put(resolvedKey, resolvedValue)(
            _ => {
              logger.debug(s"session user id: #${session.userId}, after cache.put")
              logAndExecuteNext(session, resolvedRequestName, startTime,
                ctx.coreComponents.clock.nowMillis, OK, next, None, None)
            },
            ex => logAndExecuteNext(session, resolvedRequestName, startTime,
              ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage))
          )
        },
        ex => logAndExecuteNext(session, resolvedRequestName, startTime,
          ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage)
        )
      ))
      .onFailure(ex =>
        requestName(session).map { resolvedRequestName =>
          ctx.coreComponents.statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, ex)
          executeNext(session, next)
        },
      )
  }
}
