package org.apache.ignite.gatling.check

import io.gatling.commons.validation.FailureWrapper
import io.gatling.core.check.Check.PreparedCache
import io.gatling.core.check.{Check, CheckResult}
import io.gatling.core.session.Session
import org.apache.ignite.gatling.IgniteCheck

trait IgniteCheckSupport {
  def simpleCheck[K, V](f: (Map[K, V], Session) => Boolean): IgniteCheck[K, V] =
    Check.Simple(
      (response: Map[K, V], session: Session, _: PreparedCache) =>
        if (f(response, session)) {
          CheckResult.NoopCheckResultSuccess
        } else {
          "Ignite check failed".failure
        },
      None
    )

  def simpleCheck[K, V](f: Map[K, V] => Boolean): IgniteCheck[K, V] =
    Check.Simple(
      (response: Map[K, V], _: Session, _: PreparedCache) =>
        if (f(response)) {
          CheckResult.NoopCheckResultSuccess
        } else {
          "Ignite check failed".failure
        },
      None
    )
}
