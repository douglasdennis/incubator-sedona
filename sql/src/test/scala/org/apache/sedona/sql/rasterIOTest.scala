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

import org.apache.spark.sql.{DataFrame, DataFrameReader}

import org.locationtech.jts.geom.Geometry
import org.scalatest.{BeforeAndAfter, GivenWhenThen}

import java.io.File
import scala.collection.mutable

class rasterIOTest extends TestBaseScala with BeforeAndAfter with GivenWhenThen {

  val overwriteMode = "overwrite"
  val rasterWrittenPath = resourceFolder + "raster-written/"

  val originCol = "origin"
  val sourceCol = "source"
  val geomCol = "geometry"
  val heightCol = "height"
  val widthCol = "width"
  val dataCol = "data"
  val bandsCol = "nBands"

  val geotiffFormat = "geotiff"
  val readFromCRSOption = "readFromCRS"
  val readToCRSOption = "readToCRS"
  val dropInvalidOption = "dropInvalid"
  val getBandExpr = "RS_GetBand(data, 1, nBands) as targetBand"

  val polygonWkt = "POLYGON ((-117.64141128097314 33.94356351407699, -117.64141128097314 33.664978146501284, -117.30939395196258 33.664978146501284, -117.30939395196258 33.94356351407699, -117.64141128097314 33.94356351407699))"

  val epsg4499 = "EPSG:4499"
  val epsg4326 = "EPSG:4326"

  def getRasterDfWithGeometry(path: String, otherOptions: DataFrameReader => DataFrameReader = identity): DataFrame = {
    val df = otherOptions(sparkSession.read.format(geotiffFormat).option(dropInvalidOption, true)).load(path)
    df.selectExpr(s"image.origin as $originCol", s"ST_GeomFromWKT(image.geometry) as $geomCol", s"image.height as $heightCol", s"image.width as $widthCol", s"image.data as $dataCol", s"image.nBands as $bandsCol")
  }

  def getRasterDfWithWkt(path: String, otherOptions: DataFrameReader => DataFrameReader = identity): DataFrame = {
    val df = otherOptions(sparkSession.read.format(geotiffFormat).option(dropInvalidOption, true)).load(path)
    df.selectExpr(s"image.origin as $originCol", s"image.geometry as $geomCol", s"image.height as $heightCol", s"image.width as $widthCol", s"image.data as $dataCol", s"image.nBands as $bandsCol")
  }

  def countTiffFiles(loadPath: String): Int ={
    var count = 0
    val tempFile = new File(loadPath)
    val fileList = tempFile.listFiles()
    if (fileList == null) { 0 }
    else {
      (0 until fileList.length).map(i => {
        if (fileList(i).isDirectory) countTiffFiles(fileList(i).getAbsolutePath)
        else if (fileList(i).getAbsolutePath.endsWith(".tiff") || fileList(i).getAbsolutePath.endsWith(".tif")) 1
        else 0
      }).sum
    }
  }

  var rasterdatalocation: String = resourceFolder + "raster/"
  val test3Location: String = resourceFolder + "raster/test3.tif"

  describe("Raster IO test") {
    it("Should Pass geotiff loading without readFromCRS and readToCRS") {
      var df = getRasterDfWithGeometry(rasterdatalocation)
      assert(df.first().getAs[Geometry](geomCol).toText == "POLYGON ((-13095782 4021226.5, -13095782 3983905, -13058822 3983905, -13058822 4021226.5, -13095782 4021226.5))")
      assert(df.first().getAs[Int](heightCol) == 517)
      assert(df.first().getAs[Int](widthCol) == 512)
      assert(df.first().getAs[Int](bandsCol) == 1)
      val blackBand = df.first().getAs[mutable.WrappedArray[Double]](4)
      val line1 = blackBand.slice(0, 512)
      val line2 = blackBand.slice(512, 1024)
      assert(line1(0) == 0.0) // The first value at line 1 is black
      assert(line2(159) == 0.0 && line2(160) == 123.0) // In the second line, value at 159 is black and at 160 is not black
    }

    it("Should Pass geotiff loading with readToCRS") {
      var df = getRasterDfWithGeometry(rasterdatalocation, _.option(readToCRSOption, epsg4326))
      assert(df.first().getAs[Geometry](geomCol).toText == polygonWkt)
      assert(df.first().getAs[Int](heightCol) == 517)
      assert(df.first().getAs[Int](widthCol) == 512)
      assert(df.first().getAs[Int](bandsCol) == 1)
      val blackBand = df.first().getAs[mutable.WrappedArray[Double]](4)
      val line1 = blackBand.slice(0, 512)
      val line2 = blackBand.slice(512, 1024)
      assert(line1(0) == 0.0) // The first value at line 1 is black
      assert(line2(159) == 0.0 && line2(160) == 123.0) // In the second line, value at 159 is black and at 160 is not black
    }

    it("Should Pass geotiff loading with readFromCRS") {
      var df = getRasterDfWithGeometry(rasterdatalocation, _.option(readFromCRSOption, epsg4499))
      assert(df.first().getAs[Geometry](geomCol).toText == "POLYGON ((-13095782 4021226.5, -13095782 3983905, -13058822 3983905, -13058822 4021226.5, -13095782 4021226.5))")
      assert(df.first().getAs[Int](heightCol) == 517)
      assert(df.first().getAs[Int](widthCol) == 512)
      assert(df.first().getAs[Int](bandsCol) == 1)
      val blackBand = df.first().getAs[mutable.WrappedArray[Double]](4)
      val line1 = blackBand.slice(0, 512)
      val line2 = blackBand.slice(512, 1024)
      assert(line1(0) == 0.0) // The first value at line 1 is black
      assert(line2(159) == 0.0 && line2(160) == 123.0) // In the second line, value at 159 is black and at 160 is not black
    }

    it("Should Pass geotiff loading with readFromCRS and readToCRS") {
      var df = getRasterDfWithGeometry(rasterdatalocation, _.option(readFromCRSOption, epsg4499).option(readToCRSOption, epsg4326))
      assert(df.first().getAs[Geometry](geomCol).toText == polygonWkt)
      assert(df.first().getAs[Int](heightCol) == 517)
      assert(df.first().getAs[Int](widthCol) == 512)
      assert(df.first().getAs[Int](bandsCol) == 1)
      val blackBand = df.first().getAs[mutable.WrappedArray[Double]](4)
      val line1 = blackBand.slice(0, 512)
      val line2 = blackBand.slice(512, 1024)
      assert(line1(0) == 0.0) // The first value at line 1 is black
      assert(line2(159) == 0.0 && line2(160) == 123.0) // In the second line, value at 159 is black and at 160 is not black
    }

    it("Should Pass geotiff loading with all read options") {
      var df = getRasterDfWithGeometry(rasterdatalocation, _.option(readFromCRSOption, epsg4499).option(readToCRSOption, epsg4326).option("disableErrorInCRS", true))
      assert(df.first().getAs[Geometry](geomCol).toText == polygonWkt)
      assert(df.first().getAs[Int](heightCol) == 517)
      assert(df.first().getAs[Int](widthCol) == 512)
      assert(df.first().getAs[Int](bandsCol) == 1)
      val blackBand = df.first().getAs[mutable.WrappedArray[Double]](4)
      val line1 = blackBand.slice(0, 512)
      val line2 = blackBand.slice(512, 1024)
      assert(line1(0) == 0.0) // The first value at line 1 is black
      assert(line2(159) == 0.0 && line2(160) == 123.0) // In the second line, value at 159 is black and at 160 is not black
    }

    it("should pass RS_GetBand") {
      var df = getRasterDfWithWkt(rasterdatalocation).select(dataCol, bandsCol)
      df = df.selectExpr(getBandExpr)
      assert(df.first().getAs[mutable.WrappedArray[Double]](0).length == 512 * 517)
    }

    it("should pass RS_Base64") {
      var df = getRasterDfWithGeometry(rasterdatalocation)
      df = df.selectExpr(getBandExpr, widthCol,heightCol)
      df.createOrReplaceTempView(geotiffFormat)
      df = sparkSession.sql("Select RS_base64(height, width, targetBand, RS_Array(height*width, 0), RS_Array(height*width, 0)) as encodedstring from geotiff")
//      printf(df.first().getAs[String](0))
    }

    it("should pass RS_HTML") {
      var df = getRasterDfWithGeometry(rasterdatalocation)
      df = df.selectExpr(getBandExpr, widthCol,heightCol)
      df.createOrReplaceTempView(geotiffFormat)
      df = sparkSession.sql("Select RS_base64(height, width, targetBand, RS_Array(height*width, 0.0), RS_Array(height*width, 0.0)) as encodedstring from geotiff")
      df = df.selectExpr("RS_HTML(encodedstring, '300') as htmlstring" )
      assert(df.first().getAs[String](0).startsWith("<img src=\"data:image/png;base64,iVBORw"))
      assert(df.first().getAs[String](0).endsWith("/>"))    }

    it("should pass RS_GetBand for length of Band 2") {
      var df = getRasterDfWithWkt(test3Location).select(dataCol, bandsCol)
      df = df.selectExpr("RS_GetBand(data, 2, nBands) as targetBand")
      assert(df.first().getAs[mutable.WrappedArray[Double]](0).length == 32 * 32)
    }

    it("should pass RS_GetBand for elements of Band 2") {
      var df = getRasterDfWithWkt(test3Location).select(dataCol, bandsCol)
      df = df.selectExpr("RS_GetBand(data, 2, nBands) as targetBand")
      assert(df.first().getAs[mutable.WrappedArray[Double]](0)(1) == 956.0)
    }

    it("should pass RS_GetBand for elements of Band 4") {
      var df = getRasterDfWithWkt(test3Location).select(dataCol, bandsCol)
      df = df.selectExpr("RS_GetBand(data, 4, nBands) as targetBand")
      assert(df.first().getAs[mutable.WrappedArray[Double]](0)(2) == 0.0)
    }

    it("Should Pass geotiff file writing with coalesce") {
      var df = getRasterDfWithWkt(rasterdatalocation, _.option(readToCRSOption, epsg4326))
      val savePath = rasterWrittenPath
      df.coalesce(1).write.mode(overwriteMode).format(geotiffFormat).save(savePath)

      var loadPath = savePath
      val tempFile = new File(loadPath)
      val fileList = tempFile.listFiles()
      for (i <- 0 until fileList.length) {
        if (fileList(i).isDirectory) loadPath = fileList(i).getAbsolutePath
      }

      var dfWritten = sparkSession.read.format(geotiffFormat).option(dropInvalidOption, true).load(loadPath)
      dfWritten = dfWritten.selectExpr("image.origin as origin","ST_GeomFromWkt(image.geometry) as Geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
      val rowFirst = dfWritten.first()
      assert(rowFirst.getInt(2) == 517)
      assert(rowFirst.getInt(3) == 512)
      assert(rowFirst.getInt(5) == 1)

      val blackBand = rowFirst.getAs[mutable.WrappedArray[Double]](4)
      val line1 = blackBand.slice(0, 512)
      val line2 = blackBand.slice(512, 1024)
      assert(line1(0) == 0.0) // The first value at line 1 is black
      assert(line2(159) == 0.0 && line2(160) == 123.0) // In the second line, value at 159 is black and at 160 is not black
    }

    it("Should Pass geotiff file writing with writeToCRS") {
      var df = getRasterDfWithWkt(rasterdatalocation)
      val savePath = rasterWrittenPath
      df.coalesce(1).write.mode(overwriteMode).format(geotiffFormat).option("writeToCRS", epsg4499).save(savePath)

      var loadPath = savePath
      val tempFile = new File(loadPath)
      val fileList = tempFile.listFiles()
      for (i <- 0 until fileList.length) {
        if (fileList(i).isDirectory) loadPath = fileList(i).getAbsolutePath
      }

      var dfWritten = sparkSession.read.format(geotiffFormat).option(dropInvalidOption, true).load(loadPath)
      dfWritten = dfWritten.selectExpr("image.origin as origin","ST_GeomFromWkt(image.geometry) as Geom", "image.height as height", "image.width as width", "image.data as data", "image.nBands as bands")
      val rowFirst = dfWritten.first()
      assert(rowFirst.getInt(2) == 517)
      assert(rowFirst.getInt(3) == 512)
      assert(rowFirst.getInt(5) == 1)

      val blackBand = rowFirst.getAs[mutable.WrappedArray[Double]](4)
      val line1 = blackBand.slice(0, 512)
      val line2 = blackBand.slice(512, 1024)
      assert(line1(0) == 0.0) // The first value at line 1 is black
      assert(line2(159) == 0.0 && line2(160) == 123.0) // In the second line, value at 159 is black and at 160 is not black
    }

    it("Should Pass geotiff file writing without coalesce") {
      var df = getRasterDfWithWkt(rasterdatalocation)
      val savePath = rasterWrittenPath
      df.write.mode(overwriteMode).format(geotiffFormat).save(savePath)

      var imageCount = countTiffFiles(savePath)

      assert(imageCount == 3)
    }

    it("Should Pass geotiff file writing with nested schema") {
      val df = getRasterDfWithWkt(rasterdatalocation)
      val savePath = rasterWrittenPath
      df.write.mode(overwriteMode).format(geotiffFormat).save(savePath)

      var imageCount = countTiffFiles(savePath)

      assert(imageCount == 3)
    }

    it("Should Pass geotiff file writing with renamed fields") {
      var df = getRasterDfWithWkt(rasterdatalocation).withColumnRenamed(originCol, sourceCol)
      val savePath = rasterWrittenPath
      df.write
        .mode(overwriteMode)
        .format(geotiffFormat)
        .option("fieldOrigin", sourceCol)
        .option("fieldGeometry", geomCol)
        .option("fieldNBands", bandsCol)
        .save(savePath)

      var imageCount = countTiffFiles(savePath)

      assert(imageCount == 3)
    }

    it("Should Pass geotiff file writing with nested schema and renamed fields") {
      var df = sparkSession.read.format(geotiffFormat).option(dropInvalidOption, true).load(rasterdatalocation)
      df = df.selectExpr("image as tiff_image")
      val savePath = rasterWrittenPath
      df.write
        .mode(overwriteMode)
        .format(geotiffFormat)
        .option("fieldImage", "tiff_image")
        .save(savePath)

      var imageCount = countTiffFiles(savePath)

      assert(imageCount == 3)
    }

    it("Should Pass geotiff file writing with converted geometry") {
      var df = getRasterDfWithGeometry(rasterdatalocation).withColumnRenamed(originCol, sourceCol)
      val savePath = rasterWrittenPath
      df.write
        .mode(overwriteMode)
        .format(geotiffFormat)
        .option("fieldOrigin", sourceCol)
        .option("fieldGeometry", geomCol)
        .option("fieldNBands", bandsCol)
        .save(savePath)

      var imageCount = countTiffFiles(savePath)

      assert(imageCount == 3)
    }

    it("Should Pass geotiff file writing with handling invalid schema") {
      var df = getRasterDfWithWkt(rasterdatalocation).drop(bandsCol)
      val savePath = rasterWrittenPath

      try {
        df.write
          .mode(overwriteMode)
          .format(geotiffFormat)
          .option("fieldImage", "tiff_image")
          .save(savePath)
      }
      catch {
        case e: IllegalArgumentException => {
          assert(e.getMessage == "Invalid GeoTiff Schema")
        }
      }
    }

  }
}
