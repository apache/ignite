/* @scala.file.header */

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.tests

import org.gridgain.scalar._
import scalar._
import org.scalatest.matchers._
import org.scalatest._
import junit.JUnitRunner
import org.gridgain.grid.cache._
import org.gridgain.grid._
import org.junit.runner.RunWith

/**
 * Tests for Scalar cache queries API.
 */
@RunWith(classOf[JUnitRunner])
class ScalarCacheQueriesSpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll {
    /** Entries count. */
    private val ENTRY_CNT = 10

    /** Words. */
    private val WORDS = List("", "one", "two", "three", "four", "five",
        "six", "seven", "eight", "nine", "ten")

    /** Node. */
    private var n: ClusterNode = null

    /** Cache. */
    private var c: GridCache[Int, ObjectValue] = null

    /**
     * Start node and put data to cache.
     */
    override def beforeAll() {
        n = start("modules/scalar/src/test/resources/spring-cache.xml").cluster().localNode

        c = cache$[Int, ObjectValue].get

        (1 to ENTRY_CNT).foreach(i => c.putx(i, ObjectValue(i, "str " + WORDS(i))))

        assert(c.size == ENTRY_CNT)

        c.foreach(e => println(e.getKey + " -> " + e.getValue))
    }

    /**
     * Stop node.
     */
    override def afterAll() {
        stop()
    }

    behavior of "Scalar cache queries API"

    it should "correctly execute SCAN queries" in {
        var res = c.scan(classOf[ObjectValue], (k: Int, v: ObjectValue) => k > 5 && v.intVal < 8)

        assert(res.size == 2)

        res.foreach(t => assert(t._1 > 5 && t._1 < 8 && t._1 == t._2.intVal))

        res = c.scan((k: Int, v: ObjectValue) => k > 5 && v.intVal < 8)

        assert(res.size == 2)

        res.foreach(t => assert(t._1 > 5 && t._1 < 8 && t._1 == t._2.intVal))

        res = c.scan(classOf[ObjectValue], (k: Int, v: ObjectValue) => k > 5 && v.intVal < 8)

        assert(res.size == 2)

        res.foreach(t => assert(t._1 > 5 && t._1 < 8 && t._1 == t._2.intVal))

        res = c.scan((k: Int, v: ObjectValue) => k > 5 && v.intVal < 8)

        assert(res.size == 2)

        res.foreach(t => assert(t._1 > 5 && t._1 < 8 && t._1 == t._2.intVal))
    }

    it should "correctly execute SQL queries" in {
        var res = c.sql(classOf[ObjectValue], "intVal > 5")

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 == t._2.intVal))

        res = c.sql(classOf[ObjectValue], "intVal > ?", 5)

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 == t._2.intVal))

        res = c.sql("intVal > 5")

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 == t._2.intVal))

        res = c.sql("intVal > ?", 5)

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 == t._2.intVal))

        res = c.sql(classOf[ObjectValue], "intVal > 5")

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 == t._2.intVal))

        res = c.sql(classOf[ObjectValue], "intVal > ?", 5)

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 == t._2.intVal))

        res.foreach(t => assert(t._1 > 5 && t._1 == t._2.intVal))

        res = c.sql("intVal > 5")

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 == t._2.intVal))

        res = c.sql("intVal > ?", 5)

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 == t._2.intVal))
    }

    it should "correctly execute TEXT queries" in {
        var res = c.text(classOf[ObjectValue], "str")

        assert(res.size == ENTRY_CNT)

        res = c.text(classOf[ObjectValue], "five")

        assert(res.size == 1)
        assert(res.head._1 == 5)

        res = c.text("str")

        assert(res.size == ENTRY_CNT)

        res = c.text("five")

        assert(res.size == 1)
        assert(res.head._1 == 5)

        res = c.text(classOf[ObjectValue], "str")

        assert(res.size == ENTRY_CNT)

        res = c.text(classOf[ObjectValue], "five")

        assert(res.size == 1)
        assert(res.head._1 == 5)

        res = c.text("str")

        assert(res.size == ENTRY_CNT)

        res = c.text("five")

        assert(res.size == 1)
        assert(res.head._1 == 5)
    }

    it should "correctly execute SCAN transform queries" in {
        var res = c.scanTransform(classOf[ObjectValue], (k: Int, v: ObjectValue) => k > 5 && v.intVal < 8,
            (v: ObjectValue) => v.intVal + 1)

        assert(res.size == 2)

        res.foreach(t => assert(t._1 > 5 && t._1 < 8 && t._1 + 1 == t._2))

        res = c.scanTransform((k: Int, v: ObjectValue) => k > 5 && v.intVal < 8, (v: ObjectValue) => v.intVal + 1)

        assert(res.size == 2)

        res.foreach(t => assert(t._1 > 5 && t._1 < 8 && t._1 + 1 == t._2))

        res = c.scanTransform(classOf[ObjectValue], (k: Int, v: ObjectValue) => k > 5 && v.intVal < 8, (v: ObjectValue) => v.intVal + 1)

        assert(res.size == 2)

        res.foreach(t => assert(t._1 > 5 && t._1 < 8 && t._1 + 1 == t._2))

        res = c.scanTransform((k: Int, v: ObjectValue) => k > 5 && v.intVal < 8, (v: ObjectValue) => v.intVal + 1)

        assert(res.size == 2)

        res.foreach(t => assert(t._1 > 5 && t._1 < 8 && t._1 + 1 == t._2))
    }

    it should "correctly execute SQL transform queries" in {
        var res = c.sqlTransform(classOf[ObjectValue], "intVal > 5", (v: ObjectValue) => v.intVal + 1)

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 + 1 == t._2))

        res = c.sqlTransform(classOf[ObjectValue], "intVal > ?", (v: ObjectValue) => v.intVal + 1, 5)

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 + 1 == t._2))

        res = c.sqlTransform("intVal > 5", (v: ObjectValue) => v.intVal + 1)

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 + 1 == t._2))

        res = c.sqlTransform("intVal > ?", (v: ObjectValue) => v.intVal + 1, 5)

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 + 1 == t._2))

        res = c.sqlTransform(classOf[ObjectValue], "intVal > 5", (v: ObjectValue) => v.intVal + 1)

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 + 1 == t._2))

        res = c.sqlTransform(classOf[ObjectValue], "intVal > ?", (v: ObjectValue) => v.intVal + 1, 5)

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 + 1 == t._2))

        res = c.sqlTransform("intVal > 5", (v: ObjectValue) => v.intVal + 1)

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 + 1 == t._2))

        res = c.sqlTransform("intVal > ?", (v: ObjectValue) => v.intVal + 1, 5)

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t._1 > 5 && t._1 + 1 == t._2))
    }

    it should "correctly execute TEXT transform queries" in {
        var res = c.textTransform(classOf[ObjectValue], "str", (v: ObjectValue) => v.intVal + 1)

        assert(res.size == ENTRY_CNT)

        res.foreach(t => assert(t._1 + 1 == t._2))

        res = c.textTransform(classOf[ObjectValue], "five", (v: ObjectValue) => v.intVal + 1)

        assert(res.size == 1)
        assert(res.head._1 == 5 && res.head._2 == 6)

        res = c.textTransform("str", (v: ObjectValue) => v.intVal + 1)

        assert(res.size == ENTRY_CNT)

        res.foreach(t => assert(t._1 + 1 == t._2))

        res = c.textTransform("five", (v: ObjectValue) => v.intVal + 1)

        assert(res.size == 1)
        assert(res.head._1 == 5 && res.head._2 == 6)

        res = c.textTransform(classOf[ObjectValue], "str", (v: ObjectValue) => v.intVal + 1)

        assert(res.size == ENTRY_CNT)

        res.foreach(t => assert(t._1 + 1 == t._2))

        res = c.textTransform(classOf[ObjectValue], "five", (v: ObjectValue) => v.intVal + 1)

        assert(res.size == 1)
        assert(res.head._1 == 5 && res.head._2 == 6)

        res = c.textTransform("str", (v: ObjectValue) => v.intVal + 1)

        assert(res.size == ENTRY_CNT)

        res.foreach(t => assert(t._1 + 1 == t._2))

        res = c.textTransform("five", (v: ObjectValue) => v.intVal + 1)

        assert(res.size == 1)
        assert(res.head._1 == 5 && res.head._2 == 6)
    }

    it should "correctly execute SCAN reduce queries with two reducers" in {
        var res = c.scanReduce(classOf[ObjectValue], (k: Int, v: ObjectValue) => k > 5 && v.intVal < 8,
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 13)

        res = c.scanReduce((k: Int, v: ObjectValue) => k > 5 && v.intVal < 8,
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 13)

        res = c.scanReduce(classOf[ObjectValue], (k: Int, v: ObjectValue) => k > 5 && v.intVal < 8,
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 13)

        res = c.scanReduce((k: Int, v: ObjectValue) => k > 5 && v.intVal < 8,
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 13)
    }

    it should "correctly execute SQL reduce queries with two reducers" in {
        var res = c.sqlReduce(classOf[ObjectValue], "intVal > 5",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 40)

        res = c.sqlReduce(classOf[ObjectValue], "intVal > ?",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum, 3)

        assert(res == 49)

        res = c.sqlReduce("intVal > 5",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 40)

        res = c.sqlReduce("intVal > ?",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum, 3)

        assert(res == 49)

        res = c.sqlReduce(classOf[ObjectValue], "intVal > 5",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 40)

        res = c.sqlReduce(classOf[ObjectValue], "intVal > ?",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum, 3)

        assert(res == 49)

        res = c.sqlReduce("intVal > 5", (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum,
            (i: Iterable[Int]) => i.sum)

        assert(res == 40)

        res = c.sqlReduce("intVal > ?", (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum,
            (i: Iterable[Int]) => i.sum, 3)

        assert(res == 49)
    }

    it should "correctly execute TEXT reduce queries with two reducers" in {
        var res = c.textReduce(classOf[ObjectValue], "str",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 55)

        res = c.textReduce(classOf[ObjectValue], "three five seven",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 15)

        res = c.textReduce("str", (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum,
            (i: Iterable[Int]) => i.sum)

        assert(res == 55)

        res = c.textReduce("three five seven", (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum,
            (i: Iterable[Int]) => i.sum)

        assert(res == 15)

        res = c.textReduce(classOf[ObjectValue], "str",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 55)

        res = c.textReduce(classOf[ObjectValue], "three five seven",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 15)

        res = c.textReduce("str", (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum,
            (i: Iterable[Int]) => i.sum)

        assert(res == 55)

        res = c.textReduce("three five seven", (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum,
            (i: Iterable[Int]) => i.sum)

        assert(res == 15)

        res = c.textReduce(classOf[ObjectValue], "str",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 55)

        res = c.textReduce(classOf[ObjectValue], "seven",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 7)

        res = c.textReduce("str",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 55)

        res = c.textReduce("seven",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 7)

        res = c.textReduce(classOf[ObjectValue], "str",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 55)

        res = c.textReduce(classOf[ObjectValue], "seven",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 7)

        res = c.textReduce("str",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 55)

        res = c.textReduce("seven",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, (i: Iterable[Int]) => i.sum)

        assert(res == 7)
    }

    it should "correctly execute SCAN reduce queries with one reducer" in {
        var res = c.scanReduceRemote(classOf[ObjectValue], (k: Int, v: ObjectValue) => k > 5 && v.intVal < 8,
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 13)

        res = c.scanReduceRemote((k: Int, v: ObjectValue) => k > 5 && v.intVal < 8,
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 13)

        res = c.scanReduceRemote(classOf[ObjectValue], (k: Int, v: ObjectValue) => k > 5 && v.intVal < 8,
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 13)

        res = c.scanReduceRemote((k: Int, v: ObjectValue) => k > 5 && v.intVal < 8,
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 13)
    }

    it should "correctly execute SQL reduce queries with one reducer" in {
        var res = c.sqlReduceRemote(classOf[ObjectValue], "intVal > 5",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 40)

        res = c.sqlReduceRemote(classOf[ObjectValue], "intVal > ?",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, 3)

        assert(res.sum == 49)

        res = c.sqlReduceRemote("intVal > 5",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 40)

        res = c.sqlReduceRemote("intVal > ?",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, 3)

        assert(res.sum == 49)

        res = c.sqlReduceRemote(classOf[ObjectValue], "intVal > 5",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 40)

        res = c.sqlReduceRemote(classOf[ObjectValue], "intVal > ?",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, 3)

        assert(res.sum == 49)

        res = c.sqlReduceRemote("intVal > 5",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 40)

        res = c.sqlReduceRemote("intVal > ?",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum, 3)

        assert(res.sum == 49)
    }

    it should "correctly execute TEXT reduce queries with one reducer" in {
        var res = c.textReduceRemote(classOf[ObjectValue], "str",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 55)

        res = c.textReduceRemote(classOf[ObjectValue], "seven",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 7)

        res = c.textReduceRemote("str",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 55)

        res = c.textReduceRemote("seven",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 7)

        res = c.textReduceRemote(classOf[ObjectValue], "str",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 55)

        res = c.textReduceRemote(classOf[ObjectValue], "seven",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 7)

        res = c.textReduceRemote("str",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 55)

        res = c.textReduceRemote("seven",
            (i: Iterable[(Int, ObjectValue)]) => i.map(_._2.intVal).sum)

        assert(res.sum == 7)
    }

    it should "correctly execute fields queries" in {
        var res = c.sqlFields(null, "select intVal from ObjectValue where intVal > 5")

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t.size == 1 && t.head.asInstanceOf[Int] > 5))

        res = c.sqlFields(null, "select intVal from ObjectValue where intVal > ?", 5)

        assert(res.size == ENTRY_CNT - 5)

        res.foreach(t => assert(t.size == 1 && t.head.asInstanceOf[Int] > 5))
    }

    it should "correctly execute queries with multiple arguments" in {
        val res = c.sql("from ObjectValue where intVal in (?, ?, ?)", 1, 2, 3)

        assert(res.size == 3)
    }
}

/**
 * Object for queries.
 */
private case class ObjectValue(
    /** Integer value. */
    @ScalarCacheQuerySqlField
    intVal: Int,

    /** String value. */
    @ScalarCacheQueryTextField
    strVal: String
) {
    override def toString: String = {
        "ObjectValue [" + intVal + ", " + strVal + "]"
    }
}
