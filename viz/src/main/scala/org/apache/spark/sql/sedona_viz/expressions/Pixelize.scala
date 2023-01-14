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
package org.apache.spark.sql.sedona_viz.expressions

import scala.jdk.CollectionConverters._ // scalastyle:ignore underscore.import
import scala.collection.mutable

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.sedona.viz.core.Serde.PixelSerializer
import org.apache.sedona.viz.utils.{ColorizeOption, RasterizationUtils, Pixel}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.sedona_viz.UDT.PixelUDT
import org.apache.spark.sql.types.{ArrayType, DataType}
import org.locationtech.jts.geom.{Envelope, Geometry, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon}

case class ST_Pixelize(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging {
  assert(inputExpressions.length <= 5)
  override def toString: String = s" **${ST_Pixelize.getClass.getName}**  "

  override def eval(input: InternalRow): Any = {
    val inputGeometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[Array[Byte]])
    val resolutionX = inputExpressions(1).eval(input).asInstanceOf[Integer]
    val resolutionY = inputExpressions(2).eval(input).asInstanceOf[Integer]
    val boundary = GeometrySerializer.deserialize(inputExpressions(3).eval(input).asInstanceOf[Array[Byte]]).getEnvelopeInternal
    val reverseCoordinate = false
    val pixels = inputGeometry match {
      case geometry: LineString => {
        RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, boundary, inputGeometry.asInstanceOf[LineString], reverseCoordinate).asScala
      }
      case geometry: Polygon => {
        RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, boundary, inputGeometry.asInstanceOf[Polygon], reverseCoordinate).asScala
      }
      case geometry: Point => {
        RasterizationUtils.FindPixelCoordinates(
          resolutionX,
          resolutionY,
          boundary,
          inputGeometry.asInstanceOf[Point],
          ColorizeOption.NORMAL,
          reverseCoordinate).asScala
      }
      case geometry: MultiLineString => {
        getMultiGeometryPixels(resolutionX, resolutionY, boundary, geometry, reverseCoordinate)
      }
      case geometry: MultiPolygon => {
        getMultiGeometryPixels(resolutionX, resolutionY, boundary, geometry, reverseCoordinate)
      }
      case geometry: MultiPoint => {
        getMultiGeometryPixels(resolutionX, resolutionY, boundary, geometry, reverseCoordinate)
      }
    }
    assert(pixels.size > 0)

    new GenericArrayData(pixels.map(f => {
      val out = new ByteArrayOutputStream()
      val kryo = new Kryo()
      val pixelSerializer = new PixelSerializer()
      val output = new Output(out)
      pixelSerializer.write(kryo, output, f._1)
      output.close()
      new GenericArrayData(out.toByteArray)
    }).toArray)
  }

  protected def getMultiGeometryPixels(
      resolutionX: Integer,
      resolutionY: Integer,
      boundary: Envelope,
      geometry: MultiLineString,
      reverseCoordinate: Boolean): mutable.Seq[Tuple2[Pixel, java.lang.Double]] = {
    var manyPixels =
      RasterizationUtils.FindPixelCoordinates(
        resolutionX,
        resolutionY,
        boundary,
        geometry.getGeometryN(0).asInstanceOf[LineString],
        reverseCoordinate)
    for {i <- 1 to geometry.getNumGeometries - 1} {
      manyPixels.addAll(
        RasterizationUtils.FindPixelCoordinates(
          resolutionX,
          resolutionY,
          boundary,
          geometry.getGeometryN(i).asInstanceOf[LineString],
          reverseCoordinate))
    }
    manyPixels.asScala
  }

  protected def getMultiGeometryPixels(
      resolutionX: Integer,
      resolutionY: Integer,
      boundary: Envelope,
      geometry: MultiPoint,
      reverseCoordinate: Boolean): mutable.Seq[Tuple2[Pixel, java.lang.Double]] = {
    var manyPixels =
      RasterizationUtils.FindPixelCoordinates(
        resolutionX,
        resolutionY,
        boundary,
        geometry.getGeometryN(0).asInstanceOf[Point],
        ColorizeOption.NORMAL,
        reverseCoordinate)
    for {i <- 1 to geometry.getNumGeometries - 1} {
      manyPixels.addAll(
        RasterizationUtils.FindPixelCoordinates(
          resolutionX,
          resolutionY,
          boundary,
          geometry.getGeometryN(i).asInstanceOf[Point],
          ColorizeOption.NORMAL,
          reverseCoordinate))
    }
    manyPixels.asScala
  }

  protected def getMultiGeometryPixels(
      resolutionX: Integer,
      resolutionY: Integer,
      boundary: Envelope,
      geometry: MultiPolygon,
      reverseCoordinate: Boolean): mutable.Seq[Tuple2[Pixel, java.lang.Double]] = {
    var manyPixels =
      RasterizationUtils.FindPixelCoordinates(
        resolutionX,
        resolutionY,
        boundary,
        geometry.getGeometryN(0).asInstanceOf[Polygon],
        reverseCoordinate)
    for {i <- 1 to geometry.getNumGeometries - 1} {
      manyPixels.addAll(
        RasterizationUtils.FindPixelCoordinates(
          resolutionX,
          resolutionY,
          boundary,
          geometry.getGeometryN(i).asInstanceOf[Polygon],
          reverseCoordinate))
    }
    manyPixels.asScala
  }

  override def dataType: DataType = ArrayType(new PixelUDT)
  override def children: Seq[Expression] = inputExpressions

  override def nullable: Boolean = false

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
