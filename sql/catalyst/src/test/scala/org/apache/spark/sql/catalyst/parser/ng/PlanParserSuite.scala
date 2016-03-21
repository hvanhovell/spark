/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.parser.ng

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.IntegerType

class PlanParserSuite extends PlanTest {
  import CatalystSqlParser._
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    comparePlans(parsePlan(sqlCommand), plan)
  }

  def intercept(sqlCommand: String, messages: String*): Unit = {
    val e = intercept[ParseException](parsePlan(sqlCommand))
    messages.foreach { message =>
      assert(e.message.contains(message))
    }
  }

  test("case insensitive") {
    val plan = table("a").select(all())
    assertEqual("sELEct * FroM a", plan)
    assertEqual("select * fRoM a", plan)
    assertEqual("SELECT * FROM a", plan)
  }

  test("show functions") {
    assertEqual("show functions", ShowFunctions(None, None))
    assertEqual("show functions foo", ShowFunctions(None, Some("foo")))
    assertEqual("show functions foo.bar", ShowFunctions(Some("foo"), Some("bar")))
    assertEqual("show functions 'foo\\\\.*'", ShowFunctions(None, Some("foo\\.*")))
    intercept("show functions foo.bar.baz", "SHOW FUNCTIONS unsupported name")
  }

  test("describe function") {
    assertEqual("describe function bar", DescribeFunction("bar", isExtended = false))
    assertEqual("describe function extended bar", DescribeFunction("bar", isExtended = true))
    assertEqual("describe function foo.bar", DescribeFunction("foo.bar", isExtended = false))
    assertEqual("describe function extended f.bar", DescribeFunction("f.bar", isExtended = true))
  }

  test("set operations") {
    val a = table("a").select(all())
    val b = table("b").select(all())

    assertEqual("select * from a union select * from b", Distinct(a.unionAll(b)))
    assertEqual("select * from a union distinct select * from b", Distinct(a.unionAll(b)))
    assertEqual("select * from a union all select * from b", a.unionAll(b))
    assertEqual("select * from a except select * from b", a.except(b))
    intercept("select * from a except all select * from b", "EXCEPT ALL is not supported.")
    assertEqual("select * from a except distinct select * from b", a.except(b))
    assertEqual("select * from a intersect select * from b", a.intersect(b))
    intercept("select * from a intersect all select * from b", "INTERSECT ALL is not supported.")
    assertEqual("select * from a intersect distinct select * from b", a.intersect(b))
  }

  test("common table expressions") {
    def cte(plan: LogicalPlan, namedPlans: (String, LogicalPlan)*): With = {
      val ctes = namedPlans.map {
        case (name, cte) =>
          name -> SubqueryAlias(name, cte)
      }.toMap
      With(plan, ctes)
    }
    assertEqual(
      "with cte1 as (select * from a) select * from cte1",
      cte(table("cte1").select(all()), "cte1" -> table("a").select(all())))
    assertEqual(
      "with cte1 (select 1) select * from cte1",
      cte(table("cte1").select(all()), "cte1" -> OneRowRelation.select(1)))
    assertEqual(
      "with cte1 (select 1), cte2 as (select * from cte1) select * from cte2",
      cte(table("cte2").select(all()),
        "cte1" -> OneRowRelation.select(1),
        "cte2" -> table("cte1").select(all())))
    intercept(
      "with cte1 (select 1), cte1 as (select 1 from cte1) select * from cte1",
      "Name 'cte1' is used for multiple common table expressions")
  }

  test("simple select query") {
    assertEqual("select 1", OneRowRelation.select(1))
    assertEqual("select a, b", OneRowRelation.select('a, 'b))
    assertEqual("select a, b from db.c", table("db", "c").select('a, 'b))
    assertEqual("select a, b from db.c where x < 1", table("db", "c").where('x < 1).select('a, 'b))
    assertEqual("select a, b from db.c having x < 1", table("db", "c").select('a, 'b).where('x < 1))
    assertEqual("select distinct a, b from db.c", Distinct(table("db", "c").select('a, 'b)))
    assertEqual("select all a, b from db.c", table("db", "c").select('a, 'b))
  }

  test("transform query spec") {
    val p = ScriptTransformation(Seq('a, 'b), "func", Seq.empty, table("e"), null)
    assertEqual("select transform(a, b) using 'func' from e where f < 10",
      p.copy(child = p.child.where('f < 10)))
    assertEqual("map(a, b) using 'func' as c, d from e",
      p.copy(output = Seq('c.string, 'd.string)))
    assertEqual("reduce(a, b) using 'func' as (c: int, d decimal(10, 0)) from e",
      p.copy(output = Seq('c.int, 'd.decimal(10, 0))))
  }

  test("multi select query") {
    assertEqual(
      "from a select * select * where s < 10",
      table("a").select(all()).unionAll(table("a").where('s < 10).select(all())))
    intercept(
      "from a select * select * from x where a.s < 10",
      "Multi-Insert queries cannot have a FROM clause in their individual SELECT statements")
    assertEqual(
      "from a insert into tbl1 select * insert into tbl2 select * where s < 10",
      table("a").select(all()).insertInto("tbl1").unionAll(
        table("a").where('s < 10).select(all()).insertInto("tbl2")))
  }

  test("query organization") {
    // Test all valid combinations of order by/sort by/distribute by/cluster by/limit/windows
    val baseSql = "select * from t"
    val basePlan = table("t").select(all())

    val ws = Map("w1" -> WindowSpecDefinition(Seq.empty, Seq.empty, UnspecifiedFrame))
    val limitWindowClauses = Seq(
      ("", (p: LogicalPlan) => p),
      (" limit 10", (p: LogicalPlan) => p.limit(10)),
      (" window w1 as ()", (p: LogicalPlan) => WithWindowDefinition(ws, p)),
      (" window w1 as () limit 10", (p: LogicalPlan) => WithWindowDefinition(ws, p).limit(10))
    )

    val orderSortDistrClusterClauses = Seq(
      ("", basePlan),
      (" order by a, b desc", basePlan.orderBy('a.asc, 'b.desc)),
      (" sort by a, b desc", basePlan.sortBy('a.asc, 'b.desc)),
      (" distribute by a, b", basePlan.distribute('a, 'b)),
      (" distribute by a sort by b", basePlan.distribute('a).sortBy('b.asc)),
      (" cluster by a, b", basePlan.distribute('a, 'b).sortBy('a.asc, 'b.asc))
    )

    orderSortDistrClusterClauses.foreach {
      case (s1, p1) =>
        limitWindowClauses.foreach {
          case (s2, pf2) =>
            assertEqual(baseSql + s1 + s2, pf2(p1))
        }
    }

    val msg = "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported"
    intercept(s"$baseSql order by a sort by a", msg)
    intercept(s"$baseSql cluster by a distribute by a", msg)
    intercept(s"$baseSql order by a cluster by a", msg)
    intercept(s"$baseSql order by a distribute by a", msg)
  }

  test("insert into") {
    val sql = "select * from t"
    val plan = table("t").select(all())
    def insert(
        partition: Map[String, Option[String]],
        overwrite: Boolean = false,
        ifNotExists: Boolean = false): LogicalPlan =
      InsertIntoTable(table("s"), partition, plan, overwrite, ifNotExists)

    // Single inserts
    assertEqual(s"insert overwrite table s $sql",
      insert(Map.empty, overwrite = true))
    assertEqual(s"insert overwrite table s if not exists $sql",
      insert(Map.empty, overwrite = true, ifNotExists = true))
    assertEqual(s"insert into s $sql",
      insert(Map.empty))
    assertEqual(s"insert into table s partition (c = 'd', e = 1) $sql",
      insert(Map("c" -> Option("d"), "e" -> Option("1"))))
    assertEqual(s"insert overwrite table s partition (c = 'd', x) if not exists $sql",
      insert(Map("c" -> Option("d"), "x" -> None), overwrite = true, ifNotExists = true))

    // Multi insert
    val plan2 = table("t").where('x > 5).select(all())
    assertEqual("from t insert into s select * limit 1 insert into u select * where x > 5",
      InsertIntoTable(
        table("s"), Map.empty, plan.limit(1), overwrite = false, ifNotExists = false).unionAll(
        InsertIntoTable(
          table("u"), Map.empty, plan2, overwrite = false, ifNotExists = false)))
  }

  test("aggregation") {
    val sql = "select a, b, sum(c) as c from d group by a, b"

    // Normal
    assertEqual(sql, table("d").groupBy('a, 'b)('a, 'b, 'sum.function('c).as("c")))

    // Cube
    assertEqual(s"$sql with cube",
      table("d").groupBy(Cube(Seq('a, 'b)))('a, 'b, 'sum.function('c).as("c")))

    // Rollup
    assertEqual(s"$sql with rollup",
      table("d").groupBy(Rollup(Seq('a, 'b)))('a, 'b, 'sum.function('c).as("c")))

    // Grouping Sets
    assertEqual(s"$sql grouping sets((a, b), (a), ())",
      GroupingSets(Seq(0, 1, 3), Seq('a, 'b), table("d"), Seq('a, 'b, 'sum.function('c).as("c"))))
    intercept(s"$sql grouping sets((a, b), (c), ())",
      "c doesn't show up in the GROUP BY list")
  }

  test("limit") {
    val sql = "select * from t"
    val plan = table("t").select(all())
    assertEqual(s"$sql limit 10", plan.limit(10))
    assertEqual(s"$sql limit cast(9 / 4 as int)", plan.limit(Cast(Literal(9) / 4, IntegerType)))
  }

  test("window spec") {
    // Note that WindowSpecs are testing in the ExpressionParserSuite
    val sql = "select * from t"
    val plan = table("t").select(all())
    val spec = WindowSpecDefinition(Seq('a, 'b), Seq('c.asc),
      SpecifiedWindowFrame(RowFrame, ValuePreceding(1), ValueFollowing(1)))

    // Test window resolution.
    val ws1 = Map("w1" -> spec, "w2" -> spec, "w3" -> spec)
    assertEqual(
      s"""$sql
         |window w1 as (partition by a, b order by c rows between 1 preceding and 1 following),
         |       w2 as w1,
         |       w3 as w1""".stripMargin,
      WithWindowDefinition(ws1, plan))

    // Fail with no reference.
    intercept(s"$sql window w2 as w1", "Cannot resolve window reference 'w1'")

    // Fail when resolved reference is not a window spec.
    intercept(
      s"""$sql
         |window w1 as (partition by a, b order by c rows between 1 preceding and 1 following),
         |       w2 as w1,
         |       w3 as w2""".stripMargin,
      "Window reference 'w2' is not a window specification"
    )
  }

  test("lateral view") {
    // Single lateral view
    assertEqual(
      "select * from t lateral view explode(x) expl as x",
      table("t")
        .generate(Explode('x), join = true, outer = false, Some("expl"), Seq("x"))
        .select(all()))

    // Multiple lateral views
    assertEqual(
      """select *
        |from t
        |lateral view explode(x) expl as x
        |lateral view outer json_tuple(x, y) jtup q, z""".stripMargin,
      table("t")
        .generate(Explode('x), join = true, outer = false, Some("expl"), Seq("x"))
        .generate(JsonTuple(Seq('x, 'y)), join = true, outer = true, Some("jtup"), Seq("q", "z"))
        .select(all()))

    // Multi-Insert lateral views.
    val from = table("t1").generate(Explode('x), join = true, outer = false, Some("expl"), Seq("x"))
    assertEqual(
      """from t1
        |lateral view explode(x) expl as x
        |insert into t2
        |select *
        |lateral view json_tuple(x, y) jtup q, z
        |insert into t3
        |select *
        |where s < 10
      """.stripMargin,
      Union(from
        .generate(JsonTuple(Seq('x, 'y)), join = true, outer = false, Some("jtup"), Seq("q", "z"))
        .select(all())
        .insertInto("t2"),
        from.where('s < 10).select(all()).insertInto("t3")))

    // Unsupported generator.
    intercept(
      "select * from t lateral view posexplode(x) posexpl as x, y",
      "Generator function 'posexplode' is not supported")
  }

  test("joins") {
    val testUnconditionalJoin = (sql: String, jt: JoinType) => {
      assertEqual(
        s"select * from t as tt $sql u",
        table("t").as("tt").join(table("u"), jt, None).select(all()))
    }
    val testConditionalJoin = (sql: String, jt: JoinType) => {
      assertEqual(
        s"select * from t $sql u as uu on a = b",
        table("t").join(table("u").as("uu"), jt, Option('a === 'b)).select(all()))
    }
    val testNaturalJoin = (sql: String, jt: JoinType) => {
      assertEqual(
        s"select * from t tt natural $sql u as uu",
        table("t").as("tt").join(table("u").as("uu"), NaturalJoin(jt), None).select(all()))
    }
    val testUsingJoin = (sql: String, jt: JoinType) => {
      assertEqual(
        s"select * from t $sql u using(a, b)",
        table("t").join(table("u"), UsingJoin(jt, Seq('a.attr, 'b.attr)), None).select(all()))
    }
    val testAll = Seq(testUnconditionalJoin, testConditionalJoin, testNaturalJoin, testUsingJoin)

    def test(sql: String, jt: JoinType, tests: Seq[(String, JoinType) => Unit]): Unit = {
      tests.foreach(_(sql, jt))
    }
    test("cross join", Inner, Seq(testUnconditionalJoin))
    test(",", Inner, Seq(testUnconditionalJoin))
    test("join", Inner, testAll)
    test("inner join", Inner, testAll)
    test("left join", LeftOuter, testAll)
    test("left outer join", LeftOuter, testAll)
    test("right join", RightOuter, testAll)
    test("right outer join", RightOuter, testAll)
    test("full join", FullOuter, testAll)
    test("full outer join", FullOuter, testAll)
  }

  test("sampled relations") {
    val sql = "select * from t"
    assertEqual(s"$sql tablesample(100 rows)",
      table("t").limit(100).select(all()))
    assertEqual(s"$sql as x tablesample(43 percent)",
      Sample(0, .43d, withReplacement = false, 10L, table("t").as("x"))(true).select(all()))
    assertEqual(s"$sql as x tablesample(bucket 4 out of 10)",
      Sample(0, .4d, withReplacement = false, 10L, table("t").as("x"))(true).select(all()))
    intercept(s"$sql as x tablesample(bucket 4 out of 10 on x)",
      "TABLESAMPLE(BUCKET x OUT OF y ON id) is not supported")
    intercept(s"$sql as x tablesample(bucket 11 out of 10)",
      s"Sampling fraction (${11.0/10.0}) must be on interval [0, 1]")
  }

  test("sub-query") {
    val plan = table("t0").select('id)
    assertEqual("select id from (t0)", plan)
    assertEqual("select id from ((((((t0))))))", plan)
    assertEqual(
      "(select * from t1) union distinct (select * from t2)",
      Distinct(table("t1").select(all()).unionAll(table("t2").select(all()))))
    assertEqual(
      "select * from ((select * from t1) union (select * from t2)) t",
      Distinct(table("t1").select(all()).unionAll(table("t2").select(all()))).as("t").select(all()))
    assertEqual(
      """select  id
        |from (((select id from t0)
        |       union all
        |       (select  id from t0))
        |      union all
        |      (select id from t0)) as u_1
      """.stripMargin,
      plan.unionAll(plan).unionAll(plan).as("u_1").select('id))
  }

  test("scalar sub-query") {
    assertEqual(
      "select (select max(b) from s) ss from t",
      table("t").select(ScalarSubquery(table("s").select('max.function('b))).as("ss")))
    assertEqual(
      "select * from t where a = (select b from s)",
      table("t").where('a === ScalarSubquery(table("s").select('b))).select(all()))
    assertEqual(
      "select g from t group by g having a > (select b from s)",
      table("t").groupBy('g)('g).where('a > ScalarSubquery(table("s").select('b))))
  }

  test("table reference") {
    assertEqual("table t", table("t"))
    assertEqual("table d.t", table("d", "t"))
  }

  test("inline table") {
    assertEqual("values 1, 2, 3, 4", LocalRelation.fromExternalRows(
      Seq('col1.int),
      Seq(1, 2, 3, 4).map(x => Row(x))))
    assertEqual(
      "values (1, 'a'), (2, 'b'), (3, 'c') as tbl(a, b)",
      LocalRelation.fromExternalRows(
        Seq('a.int, 'b.string),
        Seq((1, "a"), (2, "b"), (3, "c")).map(x => Row(x._1, x._2))).as("tbl"))
    intercept("values (a, 'a'), (b, 'b')",
      "All expressions in an inline table must be constants.")
    intercept("values (1, 'a'), (2, 'b') as tbl(a, b, c)",
      "Number of aliases must match the number of fields in an inline table.")
    intercept[ArrayIndexOutOfBoundsException](parsePlan("values (1, 'a'), (2, 'b', 5Y)"))
  }
}
