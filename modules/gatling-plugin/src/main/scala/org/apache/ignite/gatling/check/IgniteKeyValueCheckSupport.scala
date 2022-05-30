package org.apache.ignite.gatling.check

import io.gatling.commons.validation.{FailureWrapper, SuccessWrapper, Validation}
import io.gatling.core.check.Check.PreparedCache
import io.gatling.core.check.{Check, CheckBuilder, CheckMaterializer, CheckResult, Extractor, Preparer, identityPreparer}
import io.gatling.core.session.{Expression, ExpressionSuccessWrapper, Session}
import org.apache.ignite.gatling.IgniteCheck

import scala.annotation.implicitNotFound

class IgniteKeyValueCheckSupport {

  trait IgniteAllKeyValueCheckType

  type AllKeyValueResult[K, V] = Map[K, V]

  def simpleCheck[K, V](f: (AllKeyValueResult[K, V], Session) => Boolean): IgniteCheck[K, V] =
    Check.Simple(
      (response: AllKeyValueResult[K, V], session: Session, _: PreparedCache) =>
        if (f(response, session)) {
          CheckResult.NoopCheckResultSuccess
        } else {
          "Ignite check failed".failure
        },
      None
    )

  def simpleCheck[K, V](f: AllKeyValueResult[K, V] => Boolean): IgniteCheck[K, V] =
    Check.Simple(
      (response: AllKeyValueResult[K, V], _: Session, _: PreparedCache) =>
        if (f(response)) {
          CheckResult.NoopCheckResultSuccess
        } else {
          "Ignite check failed".failure
        },
      None
    )

  @implicitNotFound("Could not find a CheckMaterializer. This check might not be valid for Ignite.")
  implicit def checkBuilder2IgniteCheck[T, P, K, V](
                                                     checkBuilder: CheckBuilder[T, P]
                                                   )(implicit materializer: CheckMaterializer[T, IgniteCheck[K, V], AllKeyValueResult[K, V], P]): IgniteCheck[K, V] =
    checkBuilder.build(materializer)

  @implicitNotFound("Could not find a CheckMaterializer. This check might not be valid for Ignite.")
  implicit def validate2IgniteCheck[T, P, X, K, V](
                                                    validate: CheckBuilder.Validate[T, P, X]
                                                  )(implicit materializer: CheckMaterializer[T, IgniteCheck[K, V], AllKeyValueResult[K, V], P]): IgniteCheck[K, V] =
    validate.exists

  @implicitNotFound("Could not find a CheckMaterializer. This check might not be valid for Ignite.")
  implicit def find2IgniteCheck[T, P, X, K, V](
                                                find: CheckBuilder.Find[T, P, X]
                                              )(implicit materializer: CheckMaterializer[T, IgniteCheck[K, V], AllKeyValueResult[K, V], P]): IgniteCheck[K, V] =
    find.find.exists

  class AllKeyValueCheckMaterializer[K, V] extends
    CheckMaterializer[IgniteAllKeyValueCheckType, IgniteCheck[K, V], AllKeyValueResult[K, V], AllKeyValueResult[K, V]](identity) {
    override protected def preparer: Preparer[AllKeyValueResult[K, V], AllKeyValueResult[K, V]] = identityPreparer
  }

  implicit def AllKeyValueCheckMaterializer[K, V]: AllKeyValueCheckMaterializer[K, V] = new AllKeyValueCheckMaterializer[K, V]

  def AllKeyValueExtractor[K, V]: Expression[Extractor[AllKeyValueResult[K, V], AllKeyValueResult[K, V]]] =
    new Extractor[AllKeyValueResult[K, V], AllKeyValueResult[K, V]] {
      override def name: String = "allRecords"

      override def apply(prepared: AllKeyValueResult[K, V]): Validation[Option[AllKeyValueResult[K, V]]] = Some(prepared).success

      override def arity: String = "find"
    }.expressionSuccess

  def AllKeyValueResults[K, V] =
    new CheckBuilder.Find.Default[IgniteAllKeyValueCheckType, AllKeyValueResult[K, V], AllKeyValueResult[K, V]](
      AllKeyValueExtractor,
      displayActualValue = true,
    )

  def allResults[K, V]: CheckBuilder.Find.Default[IgniteAllKeyValueCheckType, AllKeyValueResult[K, V], AllKeyValueResult[K, V]] =
    AllKeyValueResults
}
