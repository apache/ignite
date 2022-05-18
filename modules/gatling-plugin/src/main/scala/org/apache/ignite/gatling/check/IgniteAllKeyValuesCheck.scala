package org.apache.ignite.gatling.check

import io.gatling.commons.validation._
import io.gatling.core.check.CheckResult
import io.gatling.core.session.{Expression, Session}
import org.apache.ignite.gatling.IgniteCheck

import java.util.{Map => JMap}

case class IgniteAllKeyValuesCheck[K, V](wrapped: IgniteCheck[K, V]) extends IgniteCheck[K, V] {

  override def check(
      response: Map[K, V],
      session: Session,
      preparedCache: JMap[Any, Any],
  ): Validation[CheckResult] = wrapped.check(response, session, preparedCache)

  override def checkIf(condition: Expression[Boolean]): IgniteCheck[K, V] = copy(
    wrapped.checkIf(condition),
  )

  override def checkIf(condition: (Map[K, V], Session) => Validation[Boolean]): IgniteCheck[K, V] = copy(
    wrapped.checkIf(condition),
  )
}
