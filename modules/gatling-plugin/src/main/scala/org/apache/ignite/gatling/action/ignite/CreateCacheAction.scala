package org.apache.ignite.gatling.action.ignite

import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.action.{Action, ChainableAction}
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.action.ActionBase
import org.apache.ignite.gatling.api.{CacheApi, IgniteApi}
import org.apache.ignite.gatling.builder.ignite.Configuration

import scala.util.Try

case class CreateCacheAction[K, V](requestName: Expression[String],
                                   cacheName: Expression[String],
                                   config: Configuration[K, V],
                                   next: Action,
                                   ctx: ScenarioContext
                                  ) extends ChainableAction with NameGen with ActionBase {

  override val name: String = genName("createCache")

  override protected def execute(session: Session): Unit = {

    val igniteApi: IgniteApi = session("igniteApi").as[IgniteApi]

    (for {
      resolvedRequestName <- requestName(session)
      resolvedCacheName <- cacheName(session)
      startTime <- ctx.coreComponents.clock.nowMillis.success
    } yield {
      Try(getOrCreateCache(igniteApi, resolvedCacheName, config)(
        _ => logAndExecuteNext(session, resolvedRequestName, startTime,
          ctx.coreComponents.clock.nowMillis, OK, next, None, None),
        ex => logAndExecuteNext(session, resolvedRequestName, startTime,
          ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage))
      )).recover(ex => {
        ctx.coreComponents.statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, ex.getMessage)
        logAndExecuteNext(session, resolvedRequestName, startTime,
          ctx.coreComponents.clock.nowMillis, KO, next, Some("ERROR"), Some(ex.getMessage))
      })
    })
      .onFailure(ex =>
        requestName(session).map { resolvedRequestName =>
          ctx.coreComponents.statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, ex)
          executeNext(session, next)
        },
      )
  }

  private def getOrCreateCache(igniteApi: IgniteApi, cacheName: String, config: Configuration[K, V]): (CacheApi[K, V] => Unit, Throwable => Unit) => Unit = {
    if (config == null)
      igniteApi.getOrCreateCache(cacheName)
    else if (config.cacheCfg != null)
      igniteApi.getOrCreateCacheByConfiguration(config.cacheCfg)
    else if (config.clientCacheCfg != null)
      igniteApi.getOrCreateCacheByClientConfiguration(config.clientCacheCfg)
    else
      igniteApi.getOrCreateCacheBySimpleConfig(cacheName, config.simpleCfg)
  }
}
