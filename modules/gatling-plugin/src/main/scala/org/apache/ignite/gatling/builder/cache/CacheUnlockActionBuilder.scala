package org.apache.ignite.gatling.builder.cache

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.action.cache.CacheUnlockAction
import org.apache.ignite.gatling.builder.IgniteActionBuilder

import java.util.concurrent.locks.Lock

case class CacheUnlockActionBuilder(requestName: Expression[String],
                                    cacheName: Expression[String],
                                    lock: Expression[Lock]) extends IgniteActionBuilder {

  override def build(ctx: ScenarioContext, next: Action): Action =
    CacheUnlockAction(requestName, cacheName, lock, next, ctx)
}