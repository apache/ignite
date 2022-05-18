package org.apache.ignite

import io.gatling.core.check.Check

package object gatling {
  type IgniteCheck[K, V] = Check[Map[K, V]]
}
