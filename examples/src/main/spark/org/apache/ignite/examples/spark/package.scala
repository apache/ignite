package org.apache.ignite.examples

/**
  * @author NIzhikov
  */
package object spark {
    object closeAfter {
        def apply[R <: AutoCloseable, T](r: R)(c: (R) ⇒ T) = {
            try {
                c(r)
            }
            finally {
                r.close
            }
        }
    }
}
