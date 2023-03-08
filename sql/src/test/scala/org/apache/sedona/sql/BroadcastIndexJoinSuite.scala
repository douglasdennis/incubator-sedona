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

import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.sedona_sql.strategy.join.BroadcastIndexJoinExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

class BroadcastIndexJoinSuite extends TestBaseScala {

  val autoBroadcastThresholdSetting = "sedona.join.autoBroadcastJoinThreshold"
  val sparkAdaptiveQueryExecutionSetting = "spark.sql.adaptive.enabled"

  val pointDfName = "pointDf"
  val pointDfOneName = "pointDf1"
  val pointDfTwoName = "pointDf2"
  val polygonDfName = "polygonDf"

  val windowExtraName = "window_extra"
  val windowExtraSum = sum(windowExtraName)

  val windowExtraTwoName = "window_extra2"
  val objectExtraTwoName = "object_extra2"

  val objectExtraName = "object_extra"
  val objectExtraSum = sum(objectExtraName)

  val radiusColumn = "radius"

  val rightOuterJoinType = "right_outer"
  val leftOuterJoinType = "left_outer"
  val leftAntiJoinType = "left_anti"
  val leftSemiJoinType = "left_semi"

  val leftAndRightPointIntersect = expr("ST_Intersects(ST_Point(l_x, l_y), ST_Point(r_x, r_y))")
  val distanceBetweenPointsLessThanRadius = expr(s"ST_Distance(${pointDfOneName}.pointshape, ${pointDfTwoName}.pointshape) < radius")
  val distanceBetweenPointsLessThanTwo = expr(s"ST_Distance(${pointDfOneName}.pointshape, ${pointDfTwoName}.pointshape) < 2")
  val distanceBetweenPointsLessThanOrEqualToTwo = expr(s"ST_Distance(${pointDfOneName}.pointshape, ${pointDfTwoName}.pointshape) <= 2")

  val pointInPolygon = expr(s"ST_Contains(${polygonDfName}.polygonshape, ${pointDfName}.pointshape)")

  val pointInPolygonAndWindowGreaterThanObject = expr(s"ST_Contains(polygonshape, pointshape) AND ${windowExtraName} > ${objectExtraName}")
  val pointInPolygonAndWindowLessThanOrEqualToObject = expr(s"ST_Contains(polygonshape, pointshape) AND ${windowExtraName} <= ${objectExtraName}")

  val windowGreaterThanObjectAndPointInPolygon = expr(s"${windowExtraName} > ${objectExtraName} AND ST_Contains(polygonshape, pointshape)")
  val bothWindowsGreaterThanObjectsAndPointInPolygon = expr(s"${windowExtraName} > ${objectExtraName} AND ${windowExtraTwoName} > ${objectExtraTwoName} AND ST_Contains(polygonshape, pointshape)")
  val bothWindowsLessThanOrEqualToObjectsAndPointInPolygon = expr(s"${windowExtraName} <= ${objectExtraName} AND ${windowExtraTwoName} <= ${objectExtraTwoName} AND ST_Contains(polygonshape, pointshape)")
  val windowLessThanOrEqualToObjectAndPointInPolygon = expr(s"${windowExtraName} <= ${objectExtraName} AND ST_Contains(polygonshape, pointshape)")

  val leftOneValue = "left_1"
  val leftTwoValue = "left_2"

  val rightTwoValue = "right_2"
  val rightThreeValue = "right_3"

  def buildLeftDf: DataFrame = {
    sparkSession.createDataFrame(Seq(
        (1.0, 1.0, leftOneValue),
        (2.0, 2.0, leftTwoValue)
    )).toDF("l_x", "l_y", "l_data")
  }

  def buildRightDf: DataFrame = {
    sparkSession.createDataFrame(Seq(
      (2.0, 2.0, rightTwoValue),
      (3.0, 3.0, rightThreeValue)
    )).toDF("r_x", "r_y", "r_data")
  }

  describe("Sedona-SQL Broadcast Index Join Test for inner joins") {

    // Using UDFs rather than lit prevents optimizations that would circumvent the checks we want to test
    val one = udf(() => 1).asNondeterministic()
    val two = udf(() => 2).asNondeterministic()

    it("Passed Correct partitioning for broadcast inner join of ST_Polygon and ST_Point") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
        pointDf.alias(pointDfName), pointInPolygon)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        polygonDf.alias(polygonDfName), pointInPolygon)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias(polygonDfName).join(
        broadcast(pointDf).alias(pointDfName), pointInPolygon)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Broadcasts the left side if both sides of inner join have a broadcast hint") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      val broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Can access attributes of both sides of broadcast inner join") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, one())

      var broadcastJoinDf = polygonDf.alias(polygonDfName).join(
        broadcast(pointDf).alias(pointDfName), pointInPolygon)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
        pointDf.alias(pointDfName), pointInPolygon)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        polygonDf.alias(polygonDfName), pointInPolygon)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000)

      broadcastJoinDf = pointDf.alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000)
    }

    it("Passed Handles extra conditions on a broadcast inner join") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, two())

      var broadcastJoinDf = pointDf
        .alias(pointDfName)
        .join(
          broadcast(polygonDf.alias(polygonDfName)),
          pointInPolygonAndWindowLessThanOrEqualToObject
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = pointDf
        .alias(pointDfName)
        .join(
          broadcast(polygonDf.alias(polygonDfName)),
          pointInPolygonAndWindowGreaterThanObject
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)

      broadcastJoinDf = pointDf
        .alias(pointDfName)
        .join(
          broadcast(polygonDf.alias(polygonDfName)),
          windowLessThanOrEqualToObjectAndPointInPolygon
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = pointDf
        .alias(pointDfName)
        .join(
          broadcast(polygonDf.alias(polygonDfName)),
          windowGreaterThanObjectAndPointInPolygon
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)
    }

    it("Passed Handles multiple extra conditions on a broadcast inner join with the ST predicate last") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one()).withColumn(windowExtraTwoName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, two()).withColumn(objectExtraTwoName, two())

      var broadcastJoinDf = pointDf
        .alias(pointDfName)
        .join(
          broadcast(polygonDf.alias(polygonDfName)),
          bothWindowsLessThanOrEqualToObjectsAndPointInPolygon
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = pointDf
        .alias(pointDfName)
        .join(
          broadcast(polygonDf.alias(polygonDfName)),
          bothWindowsGreaterThanObjectsAndPointInPolygon
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)
    }

    it("Passed ST_Distance <= distance in a broadcast inner join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed ST_Distance < distance in a broadcast inner join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanTwo)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanTwo)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed ST_Distance distance is bound to first expression in inner join") {
      val pointDf1 = buildPointDf.withColumn(radiusColumn, two())
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanRadius)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanRadius)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf2.alias(pointDfTwoName).join(
        broadcast(pointDf1).alias(pointDfOneName), distanceBetweenPointsLessThanRadius)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf2).alias(pointDfTwoName).join(
        pointDf1.alias(pointDfOneName), distanceBetweenPointsLessThanRadius)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed Correct partitioning for broadcast inner join for ST_Polygon and ST_Point with AQE enabled") {
      sparkSession.conf.set(sparkAdaptiveQueryExecutionSetting, true)
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
        pointDf.alias(pointDfName), pointInPolygon)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        polygonDf.alias(polygonDfName), pointInPolygon)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias(polygonDfName).join(
        broadcast(pointDf).alias(pointDfName), pointInPolygon)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
      sparkSession.conf.set(sparkAdaptiveQueryExecutionSetting, false)
    }

    it("Passed broadcast distance join with LineString") {
      assert(sparkSession.sql(
        """
          |select /*+ BROADCAST(a) */ *
          |from (select ST_LineFromText('LineString(1 1, 1 3, 3 3)') as geom) a
          |join (select ST_Point(2.0,2.0) as geom) b
          |on ST_Distance(a.geom, b.geom) < 0.1
          |""".stripMargin).isEmpty)
      assert(sparkSession.sql(
        """
          |select /*+ BROADCAST(a) */ *
          |from (select ST_LineFromText('LineString(1 1, 1 4)') as geom) a
          |join (select ST_Point(1.0,5.0) as geom) b
          |on ST_Distance(a.geom, b.geom) < 1.5
          |""".stripMargin).count() == 1)
    }

    it("Passed validate output rows on inner join") {
      val left = buildLeftDf

      val right = buildRightDf

      val joined = left.join(broadcast(right),
        leftAndRightPointIntersect, "inner")
      assert(joined.queryExecution.sparkPlan.collect{ case p: BroadcastIndexJoinExec => p }.size === 1)

      val rows = joined.collect()
      assert(rows.length == 1)
      assert(rows(0) == Row(2.0, 2.0, leftTwoValue, 2.0, 2.0, rightTwoValue))

      val joined2 = broadcast(left).join(right,
        leftAndRightPointIntersect, "inner")
      assert(joined.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)

      val rows2 = joined2.collect()
      assert(rows2.length == 1)
      assert(rows2(0) == Row(2.0, 2.0, leftTwoValue, 2.0, 2.0, rightTwoValue))

    }
  }

  describe("Sedona-SQL Broadcast Index Join Test for left semi joins") {

    // Using UDFs rather than lit prevents optimizations that would circumvent the checks we want to test
    val one = udf(() => 1).asNondeterministic()
    val two = udf(() => 2).asNondeterministic()

    it("Passed Correct partitioning for broadcast left semi join of ST_Polygon and ST_Point") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf.alias(pointDfName).join(
          broadcast(polygonDf.limit(limit)).alias(polygonDfName), pointInPolygon, leftSemiJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
          pointDf.limit(limit).alias(pointDfName), pointInPolygon, leftSemiJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
          polygonDf.limit(limit).alias(polygonDfName), pointInPolygon, leftSemiJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = polygonDf.alias(polygonDfName).join(
          broadcast(pointDf.limit(limit)).alias(pointDfName), pointInPolygon, leftSemiJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
        assert(broadcastJoinDf.count() == limit)
      }
    }

    it("Passed Broadcasts the right side if both sides have a broadcast hint") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      val broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon, leftSemiJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Can access attributes of left side of broadcast join") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, one())

      var broadcastJoinDf = polygonDf.alias(polygonDfName).join(
        broadcast(pointDf).alias(pointDfName), pointInPolygon, leftSemiJoinType)
      assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
        pointDf.alias(pointDfName), pointInPolygon, leftSemiJoinType)
      assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        polygonDf.alias(polygonDfName), pointInPolygon, leftSemiJoinType)
      assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000)

      broadcastJoinDf = pointDf.alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon, leftSemiJoinType)
      assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000)
    }

    it("Passed Handles extra conditions on a broadcast left semi join") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, two())

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            pointInPolygonAndWindowLessThanOrEqualToObject,
            leftSemiJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            pointInPolygonAndWindowGreaterThanObject,
            leftSemiJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 0)

        broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            windowLessThanOrEqualToObjectAndPointInPolygon,
            leftSemiJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            windowGreaterThanObjectAndPointInPolygon,
            leftSemiJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 0)
      }
    }

    it("Passed Handles multiple extra conditions on a broadcast left semi join with the ST predicate last") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one()).withColumn(windowExtraTwoName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, two()).withColumn(objectExtraTwoName, two())

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            bothWindowsLessThanOrEqualToObjectsAndPointInPolygon,
            leftSemiJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            bothWindowsGreaterThanObjectsAndPointInPolygon,
            leftSemiJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 0)
      }
    }

    it("Passed ST_Distance <= distance in a broadcast left semi join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, leftSemiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, leftSemiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2.limit(500)).alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, leftSemiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 501)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.limit(500).alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, leftSemiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 501)
    }

    it("Passed ST_Distance < distance in a broadcast left semi join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, leftSemiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, leftSemiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2.limit(500)).alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, leftSemiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 501)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.limit(500).alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, leftSemiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 501)
    }

    it("Passed ST_Distance distance is bound to first expression in left semi join") {
      val pointDf1 = buildPointDf.withColumn(radiusColumn, two())
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanRadius, leftSemiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanRadius, leftSemiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = pointDf2.alias(pointDfTwoName).join(
        broadcast(pointDf1).alias(pointDfOneName), distanceBetweenPointsLessThanRadius, leftSemiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)

      distanceJoinDf = broadcast(pointDf2).alias(pointDfTwoName).join(
        pointDf1.alias(pointDfOneName), distanceBetweenPointsLessThanRadius, leftSemiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1000)
    }

    it("Passed Correct partitioning for broadcast left semi join for ST_Polygon and ST_Point with AQE enabled") {
      sparkSession.conf.set(sparkAdaptiveQueryExecutionSetting, true)
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon, leftSemiJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
        pointDf.alias(pointDfName), pointInPolygon, leftSemiJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        polygonDf.alias(polygonDfName), pointInPolygon, leftSemiJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias(polygonDfName).join(
        broadcast(pointDf).alias(pointDfName), pointInPolygon, leftSemiJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
      sparkSession.conf.set(sparkAdaptiveQueryExecutionSetting, false)
    }

    it("Passed validate output rows on left semi join") {
      val left = buildLeftDf

      val right = buildRightDf

      val joined = left.join(broadcast(right),
        leftAndRightPointIntersect, leftSemiJoinType)
      assert(joined.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)

      val rows = joined.collect()
      assert(rows.length == 1)
      assert(rows(0) == Row(2.0, 2.0, leftTwoValue))
    }
  }

  describe("Sedona-SQL Broadcast Index Join Test for left anti joins") {

    // Using UDFs rather than lit prevents optimizations that would circumvent the checks we want to test
    val one = udf(() => 1).asNondeterministic()
    val two = udf(() => 2).asNondeterministic()

    it("Passed Correct partitioning for broadcast left anti join for ST_Polygon and ST_Point") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf.alias(pointDfName).join(
          broadcast(polygonDf.limit(limit)).alias(polygonDfName), pointInPolygon, leftAntiJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000 - limit)

        broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
          pointDf.limit(limit).alias(pointDfName), pointInPolygon, leftAntiJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000 - limit)

        broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
          polygonDf.limit(limit).alias(polygonDfName), pointInPolygon, leftAntiJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000 - limit)

        broadcastJoinDf = polygonDf.alias(polygonDfName).join(
          broadcast(pointDf.limit(limit)).alias(pointDfName), pointInPolygon, leftAntiJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000 - limit)
      }
    }

    it("Passed Broadcasts the right side if both sides have a broadcast hint") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      val broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon, leftAntiJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 0)
    }

    it("Passed Can access attributes of left side of broadcast join") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, one())

      Seq(500, 900).foreach { limit =>
        var broadcastJoinDf = polygonDf.alias(polygonDfName).join(
          broadcast(pointDf.limit(limit)).alias(pointDfName), pointInPolygon, leftAntiJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000 - limit)

        broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
          pointDf.limit(limit).alias(pointDfName), pointInPolygon, leftAntiJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000 - limit)

        broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
          polygonDf.limit(limit).alias(polygonDfName), pointInPolygon, leftAntiJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000 - limit)

        broadcastJoinDf = pointDf.alias(pointDfName).join(
          broadcast(polygonDf.limit(limit)).alias(polygonDfName), pointInPolygon, leftAntiJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000 - limit)
      }
    }

    it("Passed Handles extra conditions on a broadcast left anti join") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, two())

      Seq(500, 900).foreach { limit =>
        var broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            pointInPolygonAndWindowLessThanOrEqualToObject,
            leftAntiJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000 - limit)

        broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            pointInPolygonAndWindowGreaterThanObject,
            leftAntiJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            windowLessThanOrEqualToObjectAndPointInPolygon,
            leftAntiJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000 - limit)

        broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            windowGreaterThanObjectAndPointInPolygon,
            leftAntiJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)
      }
    }

    it("Passed Handles multiple extra conditions on a broadcast left anti join with the ST predicate last") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one()).withColumn(windowExtraTwoName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, two()).withColumn(objectExtraTwoName, two())

      var broadcastJoinDf = pointDf
        .alias(pointDfName)
        .join(
          broadcast(polygonDf.alias(polygonDfName)),
          bothWindowsLessThanOrEqualToObjectsAndPointInPolygon,
          leftAntiJoinType
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)

      broadcastJoinDf = pointDf
        .alias(pointDfName)
        .join(
          broadcast(polygonDf.alias(polygonDfName)),
          bothWindowsGreaterThanObjectsAndPointInPolygon,
          leftAntiJoinType
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed ST_Distance <= distance in a broadcast left anti join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, leftAntiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, leftAntiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2.limit(500)).alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, leftAntiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 499)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.limit(500).alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, leftAntiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 499)
    }

    it("Passed ST_Distance < distance in a broadcast left anti join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, leftAntiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, leftAntiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2.limit(500)).alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, leftAntiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 499)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.limit(500).alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, leftAntiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 499)
    }

    it("Passed ST_Distance distance is bound to first expression in left anti join") {
      val pointDf1 = buildPointDf.withColumn(radiusColumn, two())
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanRadius, leftAntiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanRadius, leftAntiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = pointDf2.alias(pointDfTwoName).join(
        broadcast(pointDf1).alias(pointDfOneName), distanceBetweenPointsLessThanRadius, leftAntiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)

      distanceJoinDf = broadcast(pointDf2).alias(pointDfTwoName).join(
        pointDf1.alias(pointDfOneName), distanceBetweenPointsLessThanRadius, leftAntiJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 0)
    }

    it("Passed Correct partitioning for broadcast left anti join for ST_Polygon and ST_Point with AQE enabled") {
      sparkSession.conf.set(sparkAdaptiveQueryExecutionSetting, true)
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon, leftAntiJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)

      broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
        pointDf.alias(pointDfName), pointInPolygon, leftAntiJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)

      broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        polygonDf.alias(polygonDfName), pointInPolygon, leftAntiJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)

      broadcastJoinDf = polygonDf.alias(polygonDfName).join(
        broadcast(pointDf).alias(pointDfName), pointInPolygon, leftAntiJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 0)
      sparkSession.conf.set(sparkAdaptiveQueryExecutionSetting, false)
    }

    it("Passed validate output rows on left anti join") {
      val left = buildLeftDf

      val right = buildRightDf

      val joined = left.join(broadcast(right),
        leftAndRightPointIntersect, leftAntiJoinType)
      assert(joined.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)

      val rows = joined.collect()
      assert(rows.length == 1)
      assert(rows(0) == Row(1.0, 1.0, leftOneValue))
    }
  }

  describe("Sedona-SQL Broadcast Index Join Test for left outer joins") {

    // Using UDFs rather than lit prevents optimizations that would circumvent the checks we want to test
    val one = udf(() => 1).asNondeterministic()
    val two = udf(() => 2).asNondeterministic()

    it("Passed Correct partitioning for broadcast left outer join for ST_Polygon and ST_Point") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf.alias(pointDfName).join(
          broadcast(polygonDf.limit(limit)).alias(polygonDfName), pointInPolygon,
          leftOuterJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
          pointDf.limit(limit).alias(pointDfName), pointInPolygon,
          leftOuterJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
          polygonDf.limit(limit).alias(polygonDfName), pointInPolygon,
          leftOuterJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = polygonDf.alias(polygonDfName).join(
          broadcast(pointDf.limit(limit)).alias(pointDfName), pointInPolygon,
          leftOuterJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
        assert(broadcastJoinDf.count() == 1000)
      }
    }

    it("Passed Broadcasts the left side if both sides of left outer join have a broadcast hint") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      val broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon, leftOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Can access attributes of both sides of broadcast left outer join") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, one())

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = polygonDf.alias(polygonDfName).join(
          broadcast(pointDf.limit(limit)).alias(pointDfName), pointInPolygon, leftOuterJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == limit)
        assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000)

        broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
          pointDf.limit(limit).alias(pointDfName), pointInPolygon, leftOuterJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == limit)
        assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000)

        broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
          polygonDf.limit(limit).alias(polygonDfName), pointInPolygon, leftOuterJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000)
        assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == limit)

        broadcastJoinDf = pointDf.alias(pointDfName).join(
          broadcast(polygonDf.limit(limit)).alias(polygonDfName), pointInPolygon, leftOuterJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000)
        assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == limit)
      }
    }

    it("Passed Handles extra conditions on a broadcast left outer join") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, two())

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            pointInPolygonAndWindowLessThanOrEqualToObject,
            leftOuterJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            pointInPolygonAndWindowGreaterThanObject,
            leftOuterJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            windowLessThanOrEqualToObjectAndPointInPolygon,
            leftOuterJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)

        broadcastJoinDf = pointDf
          .alias(pointDfName)
          .join(
            broadcast(polygonDf.limit(limit).alias(polygonDfName)),
            windowGreaterThanObjectAndPointInPolygon,
            leftOuterJoinType
          )

        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == 1000)
      }
    }

    it("Passed Handles multiple extra conditions on a broadcast left outer join with the ST predicate last") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one()).withColumn(windowExtraTwoName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, two()).withColumn(objectExtraTwoName, two())

      var broadcastJoinDf = pointDf
        .alias(pointDfName)
        .join(
          broadcast(polygonDf.alias(polygonDfName)),
          bothWindowsLessThanOrEqualToObjectsAndPointInPolygon,
          leftOuterJoinType
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = pointDf
        .alias(pointDfName)
        .join(
          broadcast(polygonDf.alias(polygonDfName)),
          bothWindowsGreaterThanObjectsAndPointInPolygon,
          leftOuterJoinType
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed ST_Distance <= distance in a broadcast left outer join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, leftOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, leftOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2.limit(500)).alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, leftOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1998)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.limit(500).alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, leftOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1998)
    }

    it("Passed ST_Distance < distance in a broadcast left outer join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, leftOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, leftOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2.limit(500)).alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, leftOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1998)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.limit(500).alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, leftOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1998)
    }

    it("Passed ST_Distance distance is bound to first expression left outer join") {
      val pointDf1 = buildPointDf.withColumn(radiusColumn, two())
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanRadius, leftOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanRadius, leftOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf2.alias(pointDfTwoName).join(
        broadcast(pointDf1).alias(pointDfOneName), distanceBetweenPointsLessThanRadius, leftOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf2).alias(pointDfTwoName).join(
        pointDf1.alias(pointDfOneName), distanceBetweenPointsLessThanRadius, leftOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed Correct partitioning for broadcast left outer join for ST_Polygon and ST_Point with AQE enabled") {
      sparkSession.conf.set(sparkAdaptiveQueryExecutionSetting, true)
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon, leftOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
        pointDf.alias(pointDfName), pointInPolygon, leftOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        polygonDf.alias(polygonDfName), pointInPolygon, leftOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias(polygonDfName).join(
        broadcast(pointDf).alias(pointDfName), pointInPolygon, leftOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
      sparkSession.conf.set(sparkAdaptiveQueryExecutionSetting, false)
    }

    it("Passed validate output rows on left outer join") {
      val left = buildLeftDf

      val right = buildRightDf

      val joined = left.join(broadcast(right),
        leftAndRightPointIntersect, leftOuterJoinType)
      assert(joined.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)

      val rows = joined.collect()
      assert(rows.length == 2)
      assert(rows(0) == Row(1.0, 1.0, leftOneValue, null, null, null))
      assert(rows(1) == Row(2.0, 2.0, leftTwoValue, 2.0, 2.0, rightTwoValue))
    }
  }

  describe("Sedona-SQL Broadcast Index Join Test for right outer joins") {

    // Using UDFs rather than lit prevents optimizations that would circumvent the checks we want to test
    val one = udf(() => 1).asNondeterministic()
    val two = udf(() => 2).asNondeterministic()

    it("Passed Correct partitioning for broadcast right outer join for ST_Polygon and ST_Point") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      Seq(500, 900, 1000).foreach { limit =>
        var broadcastJoinDf = pointDf.alias(pointDfName).join(
          broadcast(polygonDf.limit(limit)).alias(polygonDfName), pointInPolygon, rightOuterJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
          pointDf.limit(limit).alias(pointDfName), pointInPolygon, rightOuterJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
          polygonDf.limit(limit).alias(polygonDfName), pointInPolygon, rightOuterJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)

        broadcastJoinDf = polygonDf.alias(polygonDfName).join(
          broadcast(pointDf.limit(limit)).alias(pointDfName), pointInPolygon, rightOuterJoinType)
        assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
        assert(broadcastJoinDf.count() == limit)
      }
    }

    it("Passed Broadcasts the left side if both sides of a right outer join have a broadcast hint") {
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      val broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon, rightOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Can access attributes of both sides of broadcast right outer join") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, one())

      var broadcastJoinDf = polygonDf.alias(polygonDfName).join(
        broadcast(pointDf).alias(pointDfName), pointInPolygon, rightOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
        pointDf.alias(pointDfName), pointInPolygon, rightOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000)

      broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        polygonDf.alias(polygonDfName), pointInPolygon, rightOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000)

      broadcastJoinDf = pointDf.alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon, rightOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.select(objectExtraSum).collect().head(0) == 1000)
      assert(broadcastJoinDf.select(windowExtraSum).collect().head(0) == 1000)
    }

    it("Passed Handles extra conditions on a broadcast right outer join") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, two())

      var broadcastJoinDf = pointDf
        .alias(pointDfName)
        .join(
          broadcast(polygonDf.alias(polygonDfName)),
          pointInPolygonAndWindowLessThanOrEqualToObject,
          rightOuterJoinType
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf)
        .alias(pointDfName)
        .join(
          polygonDf.alias(polygonDfName),
          pointInPolygonAndWindowGreaterThanObject,
          rightOuterJoinType
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf)
        .alias(pointDfName)
        .join(
          polygonDf.alias(polygonDfName),
          windowLessThanOrEqualToObjectAndPointInPolygon,
          rightOuterJoinType
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = pointDf
        .alias(pointDfName)
        .join(
          broadcast(polygonDf.alias(polygonDfName)),
          windowGreaterThanObjectAndPointInPolygon,
          rightOuterJoinType
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed Handles multiple extra conditions on a broadcast join with the ST predicate last") {
      val polygonDf = buildPolygonDf.withColumn(windowExtraName, one()).withColumn(windowExtraTwoName, one())
      val pointDf = buildPointDf.withColumn(objectExtraName, two()).withColumn(objectExtraTwoName, two())

      var broadcastJoinDf = broadcast(pointDf)
        .alias(pointDfName)
        .join(
          polygonDf.alias(polygonDfName),
          bothWindowsLessThanOrEqualToObjectsAndPointInPolygon,
          rightOuterJoinType
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf)
        .alias(pointDfName)
        .join(
          polygonDf.alias(polygonDfName),
          bothWindowsGreaterThanObjectsAndPointInPolygon,
          rightOuterJoinType
        )

      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)
    }

    it("Passed ST_Distance <= distance in a broadcast join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, rightOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, rightOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2.limit(500)).alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, rightOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1499)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.limit(500).alias(pointDfTwoName), distanceBetweenPointsLessThanOrEqualToTwo, rightOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1499)
    }

    it("Passed ST_Distance < distance in a broadcast join") {
      val pointDf1 = buildPointDf
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, rightOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, rightOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2.limit(500)).alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, rightOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1499)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.limit(500).alias(pointDfTwoName), distanceBetweenPointsLessThanTwo, rightOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 1499)
    }

    it("Passed ST_Distance distance is bound to first expression on right outer join") {
      val pointDf1 = buildPointDf.withColumn(radiusColumn, two())
      val pointDf2 = buildPointDf

      var distanceJoinDf = pointDf1.alias(pointDfOneName).join(
        broadcast(pointDf2).alias(pointDfTwoName), distanceBetweenPointsLessThanRadius, rightOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf1).alias(pointDfOneName).join(
        pointDf2.alias(pointDfTwoName), distanceBetweenPointsLessThanRadius, rightOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = pointDf2.alias(pointDfTwoName).join(
        broadcast(pointDf1).alias(pointDfOneName), distanceBetweenPointsLessThanRadius, rightOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)

      distanceJoinDf = broadcast(pointDf2).alias(pointDfTwoName).join(
        pointDf1.alias(pointDfOneName), distanceBetweenPointsLessThanRadius, rightOuterJoinType)
      assert(distanceJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(distanceJoinDf.count() == 2998)
    }

    it("Passed Correct partitioning for broadcast right outer join for ST_Polygon and ST_Point with AQE enabled") {
      sparkSession.conf.set(sparkAdaptiveQueryExecutionSetting, true)
      val polygonDf = buildPolygonDf.repartition(3)
      val pointDf = buildPointDf.repartition(5)

      var broadcastJoinDf = pointDf.alias(pointDfName).join(
        broadcast(polygonDf).alias(polygonDfName), pointInPolygon, rightOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(polygonDf).alias(polygonDfName).join(
        pointDf.alias(pointDfName), pointInPolygon, rightOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == pointDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = broadcast(pointDf).alias(pointDfName).join(
        polygonDf.alias(polygonDfName), pointInPolygon, rightOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(broadcastJoinDf.rdd.getNumPartitions == polygonDf.rdd.getNumPartitions)
      assert(broadcastJoinDf.count() == 1000)

      broadcastJoinDf = polygonDf.alias(polygonDfName).join(
        broadcast(pointDf).alias(pointDfName), pointInPolygon, rightOuterJoinType)
      assert(broadcastJoinDf.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
      assert(broadcastJoinDf.count() == 1000)
      sparkSession.conf.set(sparkAdaptiveQueryExecutionSetting, false)
    }

    it("Passed validate output rows on right outer join") {
      val left = buildLeftDf

      val right = buildRightDf

      val joined = broadcast(left).join(right,
        leftAndRightPointIntersect, rightOuterJoinType)
      assert(joined.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)

      val rows = joined.collect()
      assert(rows.length == 2)
      assert(rows(0) == Row(2.0, 2.0, leftTwoValue, 2.0, 2.0, rightTwoValue))
      assert(rows(1) == Row(null, null, null, 3.0, 3.0, rightThreeValue))
    }
  }

  describe("Sedona-SQL Automatic broadcast") {
    it("Datasets smaller than threshold should be broadcasted") {
      val polygonDf = buildPolygonDf.repartition(3).alias("polygon")
      val pointDf = buildPointDf.repartition(5).alias("point")
      val df = polygonDf.join(pointDf, expr("ST_Contains(polygon.polygonshape, point.pointshape)"))
      sparkSession.conf.set("sedona.global.index", "true")
      sparkSession.conf.set(autoBroadcastThresholdSetting, "10mb")

      assert(df.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      sparkSession.conf.set(autoBroadcastThresholdSetting, "-1")
    }

    it("Datasets larger than threshold should not be broadcasted") {
      val polygonDf = buildPolygonDf.repartition(3).alias("polygon")
      val pointDf = buildPointDf.repartition(5).alias("point")
      val df = polygonDf.join(pointDf, expr("ST_Contains(polygon.polygonshape, point.pointshape)"))
      sparkSession.conf.set("sedona.global.index", "true")
      sparkSession.conf.set(autoBroadcastThresholdSetting, "-1")

      assert(df.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 0)
    }
  }

  describe("Sedona-SQL Broadcast join with null geometries") {
    it("Left outer join with nulls on left side") {
      import sparkSession.implicits._
      val left = Seq(("1", "POINT(1 1)"), ("2", "POINT(1 1)"), ("3", "POINT(1 1)"), ("4", null))
        .toDF("seq", "left_geom")
        .withColumn("left_geom", expr("ST_GeomFromText(left_geom)"))
      val right = Seq("POLYGON((2 0, 2 2, 0 2, 0 0, 2 0))")
        .toDF("right_geom")
        .withColumn("right_geom", expr("ST_GeomFromText(right_geom)"))
      val result = left.join(broadcast(right), expr("ST_Intersects(left_geom, right_geom)"), "left")
      assert(result.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(result.count() == 4)
    }

    it("Left anti join with nulls on left side") {
      import sparkSession.implicits._
      val left = Seq(("1", "POINT(1 1)"), ("2", "POINT(1 1)"), ("3", "POINT(1 1)"), ("4", null))
        .toDF("seq", "left_geom")
        .withColumn("left_geom", expr("ST_GeomFromText(left_geom)"))
      val right = Seq("POLYGON((2 0, 2 2, 0 2, 0 0, 2 0))")
        .toDF("right_geom")
        .withColumn("right_geom", expr("ST_GeomFromText(right_geom)"))
      val result = left.join(broadcast(right), expr("ST_Intersects(left_geom, right_geom)"), "left_anti")
      assert(result.queryExecution.sparkPlan.collect { case p: BroadcastIndexJoinExec => p }.size === 1)
      assert(result.count() == 1)
    }
  }
}
