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
package org.apache.sedona.sql.functions.geohash

import org.apache.sedona.sql.{GeometrySample, TestBaseScala}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.should.Matchers


class TestStGeoHash extends TestBaseScala with GeometrySample with GivenWhenThen with Matchers{

  import sparkSession.implicits._

  val idCol = "id"
  val geomCol = "geom"
  val geohashCol = "geohash"

  def createGeometryDataFrame(data: Seq[Tuple2[Integer, Any]]): DataFrame = {
    data.map{
        case (id, geomWkt: String) => (id, wktReader.read(geomWkt))
        case (id, _) => (id, null)
      }.toDF(idCol, geomCol)
  }

  def applyGeoHash(df: DataFrame, resolution: Integer): DataFrame = df.withColumn(geohashCol, expr(s"ST_Geohash($geomCol, $resolution)"))

  val calculatingGeoHash = "calculating geohash"
  val geohashShouldBeNull = "geohash should be null"

  describe("should correctly calculate st geohash function for 10 precision"){
    it("should return geohash"){
      Given("geometry dataframe")
      val geometryDf = Seq(
        (1, "POINT(21.4234 52.0423)", "u3r0pd0037"),
        (2, "LINESTRING (30 10, 10 30, 40 40)", "ss3y0zh7w1"),
        (4, "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))", "ssgs3y0zh7"),
        (5, "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))", "ss3y0zh7w1"),
        (6, "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", "ss1b0bh2n0"),
        (7,
          "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))",
          "ssgs3y0zh7"
        )
      ).map{
        case (id, geomWkt, expectedGeoHash) => (id, wktReader.read(geomWkt), expectedGeoHash)
      }.toDF(idCol, geomCol, "expected_geohash")

      When(calculatingGeoHash)
      val geoHash = applyGeoHash(geometryDf, 10)
        .withColumn("eq_geohash", col(geohashCol) === col("expected_geohash"))
        .select("eq_geohash")
        .distinct()
        .as[Boolean]
        .collect()
        .toList

      Then("should return correct result")
      geoHash.size shouldBe 1
      geoHash.head shouldBe true
    }

    it("should return null if column value is null"){
      Given("geometry df with null elements")
      val geometryDataFrame = createGeometryDataFrame(Seq((1, null)))

      When(calculatingGeoHash)
      val geoHashDf = geometryDataFrame.withColumn(geohashCol, expr("ST_GeoHash(geom, 10)"))

      Then("result should be null")
      geoHashDf.select(geohashCol).as[String].collect().head shouldBe null
    }

    it("should return geohash truncated to max value"){
      Given("geometry df")
      val geometryDf = createGeometryDataFrame(Seq((1, "POINT(21.427834 52.042576573)")))

      When("calculating geohash with precision exceeding maximum allowed")
      val geoHashDf = applyGeoHash(geometryDf, 21)

      Then("geohash should be truncated to maximum possible precision")
      val result = geoHashDf.select(geohashCol).distinct().as[String].collect().toList
      result.size shouldBe 1

      result.head shouldBe "u3r0pd53bxrjdsrz4fzj"

    }

    it("should return empty string when precision is negative or equal 0"){
      Given("geometry df")
      val geometryDf = createGeometryDataFrame(Seq((1, "POINT(21.427834 52.042576573)")))

      When("calculating geohash with precision exceeding maximum allowed")
      val geoHashDf = applyGeoHash(geometryDf, 0)

      Then("geohash should be truncated to maximum possible precision")
      val result = geoHashDf.select(geohashCol).distinct().as[String].collect().toList
      result.size shouldBe 1

      result.head shouldBe ""
    }

    it("should not return null for 90 < long < 180 (SEDONA-123)") {
      Given("geometry df with longitude greater than 90")
      val geometryDf = createGeometryDataFrame(Seq((1, "POINT(120.0 50.0)")))

      When(calculatingGeoHash)
      val geoHashDf = applyGeoHash(geometryDf, 12)

      Then("geohash should not be null / return expected result")
      val result = geoHashDf.select(geohashCol).distinct().as[String].collect().toList

      result.size shouldBe 1
      result.head shouldBe "y8vk6wjr4et3"
    }

    it("should return expected value for boundary case of min lat/long") {
      Given("geometry df with point at the minimum boundary")
      val geometryDf = createGeometryDataFrame(Seq((1, "POINT(-180.0 -90.0)")))

      When(calculatingGeoHash)
      val geoHashDf = applyGeoHash(geometryDf, 12)

      Then("geohash should return expected result")
      val result = geoHashDf.select(geohashCol).distinct().as[String].collect().toList

      result.size shouldBe 1
      result.head shouldBe "000000000000"
    }

    it("should return expected value for boundary case of max lat/long") {
      Given("geometry df with point at the maximum boundary")
      val geometryDf = createGeometryDataFrame(Seq((1, "POINT(180.0 90.0)")))

      When(calculatingGeoHash)
      val geoHashDf = applyGeoHash(geometryDf, 12)

      Then("geohash should return expected result")
      val result = geoHashDf.select(geohashCol).distinct().as[String].collect().toList

      result.size shouldBe 1
      result.head shouldBe "zzzzzzzzzzzz"
    }
  }

  describe("should return null when geometry contains invalid coordinates") {
    it("should return null when longitude is less than -180") {
      Given("geometry df with longitude less than -180")
      val geometryDf = createGeometryDataFrame(Seq((1, "POINT(-190.0 50.0)")))

      When(calculatingGeoHash)
      val geoHashDf = applyGeoHash(geometryDf, 1)

      Then(geohashShouldBeNull)
      val result = geoHashDf.select(geohashCol).distinct().as[String].collect().toList

      result.size shouldBe 1
      result.head shouldBe null
    }

    it("should return null when longitude is greater than 180") {
      Given("geometry df with longitude greater than 180")
      val geometryDf = createGeometryDataFrame(Seq((1, "POINT(190.0 50.0)")))

      When(calculatingGeoHash)
      val geoHashDf = applyGeoHash(geometryDf, 1)

      Then(geohashShouldBeNull)
      val result = geoHashDf.select(geohashCol).distinct().as[String].collect().toList

      result.size shouldBe 1
      result.head shouldBe null
    }

    it("should return null when latitude is less than -90") {
      Given("geometry df with latitude less than -90")
      val geometryDf = createGeometryDataFrame(Seq((1, "POINT(50.0 -100.0)")))

      When(calculatingGeoHash)
      val geoHashDf = applyGeoHash(geometryDf, 1)

      Then(geohashShouldBeNull)
      val result = geoHashDf.select(geohashCol).distinct().as[String].collect().toList

      result.size shouldBe 1
      result.head shouldBe null
    }

    it("should return null when latitude is greater than 90") {
      Given("geometry df with latitude greater than 90")
      val geometryDf = createGeometryDataFrame(Seq((1, "POINT(50.0 100.0)")))

      When(calculatingGeoHash)
      val geoHashDf = applyGeoHash(geometryDf, 1)

      Then(geohashShouldBeNull)
      val result = geoHashDf.select(geohashCol).distinct().as[String].collect().toList

      result.size shouldBe 1
      result.head shouldBe null
    }
  }
}
