/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.sql

import org.apache.spark.sql.catalyst.expressions.{EmptyRow, Literal}
import org.apache.spark.sql.sedona_sql.expressions.{ST_Contains, ST_CoveredBy, ST_Covers, ST_Crosses, ST_Disjoint, ST_Equals, ST_Intersects, ST_OrderingEquals, ST_Overlaps, ST_Point, ST_Touches, ST_Within}

class predicateTestScala extends TestBaseScala {

  val testTable = "testtable"
  val polygonTable = "polygontable"
  val pointTable = "pointtable"
  val polygonDfTable = "polygondf"
  val pointDfTable = "pointdf"

  def loadCsvIntoTable(path: String, tableName: String): Unit = {
    val csvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(path)
    csvDF.createOrReplaceTempView(tableName)
  }

  def createPolygonTable: Unit = loadCsvIntoTable(csvPolygon1InputLocation, polygonTable)
  def createPointTable: Unit = loadCsvIntoTable(csvPointInputLocation, pointTable)

  def createPointLandmarkTable: Unit = {
    createPointTable
    var pointDf = sparkSession.sql(s"select ST_Point(cast(${pointTable}._c0 as Decimal(24,20)), cast(${pointTable}._c1 as Decimal(24,20))) as arealandmark from ${pointTable}")
    pointDf.createOrReplaceTempView(pointDfTable)
  }

  def createPolygonShapeTable: Unit = {
    createPolygonTable
    var polygonDf = sparkSession.sql(s"select ST_PolygonFromEnvelope(cast(${polygonTable}._c0 as Decimal(24,20)),cast(${polygonTable}._c1 as Decimal(24,20)), cast(${polygonTable}._c2 as Decimal(24,20)), cast(${polygonTable}._c3 as Decimal(24,20))) as polygonshape from ${polygonTable}")
    polygonDf.createOrReplaceTempView(polygonDfTable)
  }

  describe("Sedona-SQL Predicate Test") {

    it("Passed ST_Contains") {
      createPointLandmarkTable

      var resultDf = sparkSession.sql(s"select * from ${pointDfTable} where ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), ${pointDfTable}.arealandmark)")
      assert(resultDf.count() == 999)
    }
    it("Passed ST_Intersects") {
      createPointTable
      var pointDf = sparkSession.sql(s"select ST_Point(cast(${pointTable}._c0 as Decimal(24,20)),cast(${pointTable}._c1 as Decimal(24,20))) as arealandmark from ${pointTable}")
      pointDf.createOrReplaceTempView(pointDfTable)

      var resultDf = sparkSession.sql(s"select * from ${pointDfTable} where ST_Intersects(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), ${pointDfTable}.arealandmark)")
      assert(resultDf.count() == 999)
    }
    it("Passed ST_Within") {
      createPointLandmarkTable

      var resultDf = sparkSession.sql(s"select * from ${pointDfTable} where ST_Within(${pointDfTable}.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))")
      assert(resultDf.count() == 999)
    }
    it("Passed ST_Covers") {
      createPointLandmarkTable

      var resultDf = sparkSession.sql(s"select * from ${pointDfTable} where ST_Covers(ST_PolygonFromEnvelope(1.0,100.0,101.0,201.0), ${pointDfTable}.arealandmark)")
      assert(resultDf.count() == 100)
    }
    it("Passed ST_CoveredBy") {
      createPointLandmarkTable

      var resultDf = sparkSession.sql(s"select * from ${pointDfTable} where ST_CoveredBy(${pointDfTable}.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,101.0,201.0))")
      assert(resultDf.count() == 100)
    }

    it("Passed ST_Equals for ST_Point") {
      // Select a point from the table and check if any point in the table is equal to the selected point.

      // Read csv to get the points table
      loadCsvIntoTable(csvPoint1InputLocation, pointTable)

      // Convert the pointtable to pointdf using ST_Point
      var pointDf = sparkSession.sql(s"select ST_Point(cast(${pointTable}._c0 as Decimal(24,20)),cast(${pointTable}._c1 as Decimal(24,20))) as point from ${pointTable}")
      pointDf.createOrReplaceTempView(pointDfTable)

      var equaldf = sparkSession.sql(s"select * from ${pointDfTable} where ST_Equals(${pointDfTable}.point, ST_Point(100.1, 200.1)) ")

      assert(equaldf.count() == 5, s"Expected 5 value but got ${equaldf.count()}")

    }

    it("Passed ST_Equals for ST_Polygon") {
      createPolygonShapeTable

      // Select a polygon from the table and check if any polygon in the table is equal to the selected polygon.

      // Selected polygon is Polygon (100.01,200.01,100.5,200.5)
      var equaldf1 = sparkSession.sql(s"select * from ${polygonDfTable} where ST_Equals(${polygonDfTable}.polygonshape, ST_PolygonFromEnvelope(100.01,200.01,100.5,200.5)) ")

      assert(equaldf1.count() == 5, s"Expected 5 value but got ${equaldf1.count()}")

      // Change the order of the polygon points (100.5,200.5,100.01,200.01)
      var equaldf2 = sparkSession.sql(s"select * from ${polygonDfTable} where ST_Equals(${polygonDfTable}.polygonshape, ST_PolygonFromEnvelope(100.5,200.5,100.01,200.01)) ")

      assert(equaldf2.count() == 5, s"Expected 5 value but got ${equaldf2.count()}")

    }

    it("Passed ST_Equals for ST_Point and ST_Polygon") {
      createPolygonShapeTable

      // Test a Point against any polygon in the table for equality.

      // Selected point is Point (91.01,191.01)
      var equaldf = sparkSession.sql(s"select * from ${polygonDfTable} where ST_Equals(${polygonDfTable}.polygonshape, ST_Point(91.01,191.01)) ")

      assert(equaldf.count() == 0, s"Expected 0 value but got ${equaldf.count()}")

    }

    it("Passed ST_Equals for ST_LineString and ST_Polygon") {
      createPolygonShapeTable

      // Test a LineString against any polygon in the table for equality.

      /* Selected LineString is ST_LineStringFromText - (100.01,200.01,100.5,200.01,100.5,200.5,100.01,200.5,100.01,200.01)
       * It forms the boundary of the polygon Polygon(100.01,200.01,100.5,200.5)
       * x1 = 100.01, y1 = 200.01, x2 = 100.5, y2 = 200.5
       * LineString(P1, P2, P3, P4) -
       * P1->100.01,200.01
       * P2->100.5,200.01
       * P3->100.5,200.5
       * P4->100.01,200.5
       * P5->100.01,200.01
       */
      val string = "100.01,200.01,100.5,200.01,100.5,200.5,100.01,200.5,100.01,200.01"

      var equaldf = sparkSession.sql(s"select * from ${polygonDfTable} where ST_Equals(${polygonDfTable}.polygonshape, ST_LineStringFromText(\'$string\', \',\')) ")

      assert(equaldf.count() == 0, s"Expected 0 value but got ${equaldf.count()}")

    }
    it("Passed ST_Equals for ST_PolygonFromEnvelope and ST_PolygonFromText") {
      createPolygonShapeTable

      // Test a Polygon formed using ST_PolygonFromText against any polygon in the table formed using ST_PolygonFromEnvelope for equality.
      // Selected Polygon is ST_PolygonFromText - Polygon(100.01,200.01,100.5,200.5) formed using ST_PolygonFromText.
      val string = "100.01,200.01,100.5,200.01,100.5,200.5,100.01,200.5,100.01,200.01"

      var equaldf = sparkSession.sql(s"select * from ${polygonDfTable} where ST_Equals(${polygonDfTable}.polygonshape, ST_PolygonFromText(\'$string\', \',\')) ")

      assert(equaldf.count() == 5, s"Expected 5 value but got ${equaldf.count()}")
    }

    it("Passed ST_Crosses") {
      var crossesTesttable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') as a,ST_GeomFromWKT('LINESTRING(1 5, 5 1)') as b")
      crossesTesttable.createOrReplaceTempView("crossesTesttable")
      var crosses = sparkSession.sql("select(ST_Crosses(a, b)) from crossesTesttable")

      var notCrossesTesttable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))') as b")
      notCrossesTesttable.createOrReplaceTempView("notCrossesTesttable")
      var notCrosses = sparkSession.sql("select(ST_Crosses(a, b)) from notCrossesTesttable")

      assert(crosses.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(!notCrosses.take(1)(0).get(0).asInstanceOf[Boolean])
    }

    it("Passed ST_Touches") {
      createPointLandmarkTable

      var resultDf = sparkSession.sql(s"select * from ${pointDfTable} where ST_Touches(${pointDfTable}.arealandmark, ST_PolygonFromEnvelope(0.0,99.0,1.1,101.1))")
      assert(resultDf.count() == 1)
    }
    it("Passed ST_Overlaps") {
      var testtable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((2.5 2.5, 2.5 4.5, 4.5 4.5, 4.5 2.5, 2.5 2.5))') as a,ST_GeomFromWKT('POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))') as b, ST_GeomFromWKT('POLYGON((5 5, 4 6, 6 6, 6 4, 5 5))') as c, ST_GeomFromWKT('POLYGON((5 5, 4 6, 6 6, 6 4, 5 5))') as d")
      testtable.createOrReplaceTempView(testTable)
      var overlaps = sparkSession.sql(s"select ST_Overlaps(a,b) from ${testTable}")
      var notoverlaps = sparkSession.sql(s"select ST_Overlaps(c,d) from ${testTable}")
      assert(overlaps.take(1)(0).get(0) == true)
      assert(notoverlaps.take(1)(0).get(0) == false)
    }
    it("Passed ST_Disjoint") {
      var testtable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))') as a,ST_GeomFromWKT('POLYGON((5 4, 6 4, 6 2, 5 2, 5 4))') as b, ST_GeomFromWKT('POLYGON((1 9, 6 6, 6 4, 1 2,1 9))') as c, ST_GeomFromWKT('POLYGON((2 5, 4 5, 4 1, 2 1, 2 5))') as d")
      testtable.createOrReplaceTempView(testTable)
      var disjoint = sparkSession.sql(s"select ST_Disjoint(a,b) from ${testTable}")
      var notdisjoint = sparkSession.sql(s"select ST_Disjoint(c,d) from ${testTable}")
      assert(disjoint.take(1)(0).get(0) == true)
      assert(notdisjoint.take(1)(0).get(0) == false)
    }

    it("Passed ST_OrderingEquals") {
      var testtable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))') as a, ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))') as b, ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 0 -2, 2 0))') as c, ST_GeomFromWKT('POLYGON((0 2, -2 0, 2 0, 0 2))') as d")
      testtable.createOrReplaceTempView(testTable)
      var orderEquals = sparkSession.sql(s"select ST_OrderingEquals(a,b) from ${testTable}")
      var notOrderEqualsDiffGeom = sparkSession.sql(s"select ST_OrderingEquals(a,c) from ${testTable}")
      var notOrderEqualsDiffOrder = sparkSession.sql(s"select ST_OrderingEquals(a,d) from ${testTable}")
      assert(orderEquals.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(!notOrderEqualsDiffGeom.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(!notOrderEqualsDiffOrder.take(1)(0).get(0).asInstanceOf[Boolean])
    }

    it("Passed edge cases of ST_Contains and ST_Covers") {
      val testtable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))') AS a, ST_GeomFromWKT('POINT(2 0)') AS b")
      testtable.createOrReplaceTempView(testTable)
      val contains = sparkSession.sql(s"select ST_Contains(a, b) from ${testTable}").take(1)(0)
      val covers = sparkSession.sql(s"select ST_Covers(a, b) from ${testTable}").take(1)(0)
      assert(!contains.get(0).asInstanceOf[Boolean])
      assert(covers.get(0).asInstanceOf[Boolean])
    }

    it("Passed edge cases of ST_Within and ST_CoveredBy") {
      val testtable = sparkSession.sql("select ST_GeomFromWKT('POLYGON((2 0, 0 2, -2 0, 2 0))') AS a, ST_GeomFromWKT('POINT(2 0)') AS b")
      testtable.createOrReplaceTempView(testTable)
      val within = sparkSession.sql(s"select ST_Within(b, a) from ${testTable}").take(1)(0)
      val coveredBy = sparkSession.sql(s"select ST_CoveredBy(b, a) from ${testTable}").take(1)(0)
      assert(!within.get(0).asInstanceOf[Boolean])
      assert(coveredBy.get(0).asInstanceOf[Boolean])
    }

    Seq(
      ST_Contains, ST_Intersects, ST_Within, ST_Covers, ST_CoveredBy, ST_Crosses,
      ST_Overlaps, ST_Touches, ST_Equals, ST_Disjoint, ST_OrderingEquals
    ).foreach { predicate =>
      it(s"Passed null handling in $predicate") {
        val point = ST_Point(Literal.create(0.0) :: Literal.create(0.0) :: Literal.create(0.0):: Nil)
        val missing = Literal.create(null)

        assert(predicate(point :: point :: Nil).eval(EmptyRow) != null)
        assert(predicate(point :: missing :: Nil).eval(EmptyRow) == null)
        assert(predicate(missing :: point :: Nil).eval(EmptyRow) == null)
        assert(predicate(missing :: missing :: Nil).eval(EmptyRow) == null)
      }
    }
  }
}
