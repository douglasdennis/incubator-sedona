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

package org.apache.sedona.sql.functions.collect

import org.apache.sedona.sql.{GeometrySample, TestBaseScala}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.should.Matchers

class TestStCollect
    extends TestBaseScala
    with GeometrySample
    with GivenWhenThen
    with Matchers {

  import sparkSession.implicits._

  val emptyGeometryCollection = "GEOMETRYCOLLECTION EMPTY"
  val geomCol = "geom"
  val idCol = "id"
  val collectedGeomCol = "collected_geom"
  val collectGeomExpr = expr(s"ST_Collect($geomCol)")

  def collectWktsFromDf(df: DataFrame): Seq[String] = {
    df.selectExpr(s"ST_AsText($collectedGeomCol)").distinct().as[String].collect()
  }

  describe("st collect workflow") {
    it("should return null when passed geometry is also null") {
      Given("data frame with empty geometries")
      val emptyGeometryDataFrame = Seq(
        (1, null),
        (2, null),
        (3, null)
      ).toDF(idCol, geomCol)

      When("running st collect on null elements")
      val withCollectedGeometries = emptyGeometryDataFrame
        .withColumn(collectedGeomCol, collectGeomExpr)

      Then("result should contain all rows null")
      collectWktsFromDf(withCollectedGeometries)
        .head shouldBe emptyGeometryCollection

    }

    it("should return null geometry when multiple passed geometries are null") {
      Given("data frame with multiple null elements")
      val emptyGeometryDataFrame = Seq(
        (1, null, null),
        (2, null, null),
        (3, null, null)
      ).toDF(idCol, "geom_left", "geom_right")

      When("running st collect on null elements")
      val withCollectedGeometries = emptyGeometryDataFrame
        .withColumn(collectedGeomCol, expr("ST_Collect(geom_left, geom_right)"))

      Then("result should be null")
      collectWktsFromDf(withCollectedGeometries)
        .head shouldBe emptyGeometryCollection
    }

    it("should not fail if any element in given array is empty") {
      Given("Dataframe with geometries and null geometries within array")
      val emptyGeometryDataFrame = Seq(
        (1, Seq(null)),
        (
          2,
          Seq(
            null,
            wktReader.read("LINESTRING(0 0, 1 1, 2 2)"),
            wktReader.read("LINESTRING(5 7, 4 3, 1 1, 0 0)")
          )
        ),
        (
          3,
          Seq(
            null,
            wktReader.read("Point(21 52)"),
            wktReader.read("POINT(45 43)")
          )
        )
      ).toDF(idCol, geomCol)

      When("running st_collect function")
      val withCollectedGeometries = emptyGeometryDataFrame
        .withColumn(collectedGeomCol, collectGeomExpr)

      Then(
        "only those non null element should be included in process of creating multigeometry"
      )
      val expectedGeoms = collectWktsFromDf(withCollectedGeometries)

      expectedGeoms should contain theSameElementsAs Seq(
        emptyGeometryCollection,
        "MULTILINESTRING ((0 0, 1 1, 2 2), (5 7, 4 3, 1 1, 0 0))",
        "MULTIPOINT ((21 52), (45 43))"
      )

    }

    it("should return multi geometry for an array of geometries") {
      Given("Dataframe with geometries and null geometries within array")
      val emptyGeometryDataFrame = Seq(
        (
          1,
          Seq(
            "POLYGON((1 2,1 4,3 4,3 2,1 2))",
            "POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 3,4 1,1.5 1))"
          ).map(wktReader.read)
        ),
        (
          2,
          Seq(
            "LINESTRING(1 2, 3 4)",
            "LINESTRING(3 4, 4 5)"
          ).map(wktReader.read)
        ),
        (
          3,
          Seq(
            "POINT(1 2)",
            "POINT(-2 3)"
          ).map(wktReader.read)
        )
      ).toDF(idCol, geomCol)

      When("running st_collect function")
      val withCollectedGeometries = emptyGeometryDataFrame
        .withColumn(collectedGeomCol, collectGeomExpr)

      Then(
        "only those non null element should be included in process of creating multigeometry"
      )
      val expectedGeoms = collectWktsFromDf(withCollectedGeometries)

      expectedGeoms should contain theSameElementsAs Seq(
        "MULTIPOLYGON (((1 2, 1 4, 3 4, 3 2, 1 2)), ((0.5 0.5, 5 0, 5 5, 0 5, 0.5 0.5), (1.5 1, 4 3, 4 1, 1.5 1)))",
        "MULTILINESTRING ((1 2, 3 4), (3 4, 4 5))",
        "MULTIPOINT ((1 2), (-2 3))"
      )
    }

    it("should return multi geometry for a list of geometries") {
      Given("Data frame with more than one geometry column")
      val geometryDf = Seq(
        (1, "POINT(21 52)", "POINT(43 34)", "POINT(12 34)", "POINT(34 67)")
      ).map {
          case (id, geomFirst, geomSecond, geomThird, geomFourth) =>
            (
              id,
              wktReader.read(geomFirst),
              wktReader.read(geomSecond),
              wktReader.read(geomThird),
              wktReader.read(geomFourth)
            )
        }
        .toDF(idCol, "geomFirst", "geomSecond", "geomThird", "geomFourth")

      When("running st collect function")
      val stCollectResult = geometryDf
        .withColumn(
          collectedGeomCol,
          expr("ST_Collect(geomFirst, geomSecond, geomThird, geomFourth)")
        )

      Then("multi geometries should be created")
      collectWktsFromDf(stCollectResult) should contain theSameElementsAs (
        Seq("MULTIPOINT ((21 52), (43 34), (12 34), (34 67))")
      )

    }

    it("should filter out null values if exists in any of passed column") {
      Given(
        "Data frame with more than one geometry column and some of them are filled with null"
      )
      val geometryDf = Seq(
        (1, null, "POINT(43 58)", null, "POINT(34 67)")
      ).map {
          case (id, geomFirst, geomSecond, geomThird, geomFourth) =>
            (
              id,
              geomFirst,
              wktReader.read(geomSecond),
              geomThird,
              wktReader.read(geomFourth)
            )
        }
        .toDF(idCol, "geomFirst", "geomSecond", "geomThird", "geomFourth")

      When("running st collect function")
      val stCollectResult = geometryDf
        .withColumn(
          collectedGeomCol,
          expr("ST_Collect(geomFirst, geomSecond, geomThird, geomFourth)")
        )

      Then("multi geometries should be created")
      collectWktsFromDf(stCollectResult) should contain theSameElementsAs (
        Seq("MULTIPOINT ((43 58), (34 67))")
      )
    }

    it("should return multitype when only one column is passed") {
      Given("data frame with one geometry column")
      val geometryDf = Seq(
        (1, "POINT(43 54)"),
        (2, "POLYGON((1 2,1 4,3 4,3 2,1 2))"),
        (3, "LINESTRING(1 2, 3 4)")
      ).map { case (id, geom) => (id, wktReader.read(geom)) }.toDF(idCol, geomCol)

      When("running on st collect on one geometry column")
      val geometryDfWithCollected = geometryDf
        .withColumn(
          collectedGeomCol,
          collectGeomExpr
        )

      Then("should return MultiType for each geometry")
      collectWktsFromDf(geometryDfWithCollected) should contain theSameElementsAs
        Seq(
          "MULTIPOINT ((43 54))",
          "MULTIPOLYGON (((1 2, 1 4, 3 4, 3 2, 1 2)))",
          "MULTILINESTRING ((1 2, 3 4))"
        )
    }
  }
}
