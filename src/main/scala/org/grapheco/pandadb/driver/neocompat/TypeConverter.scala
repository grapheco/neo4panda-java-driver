package org.grapheco.pandadb.driver.neocompat

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.{LynxFloat, LynxNull}
import org.grapheco.lynx.types.spatial.{Cartesian2D, Cartesian3D, Geographic2D, Geographic3D}
import org.grapheco.lynx.types.structural.{LynxElement, LynxNode, LynxPath, LynxRelationship}
import org.grapheco.pandadb.facade.Direction.Direction
import org.grapheco.pandadb.facade.{Direction, PandaTransaction}
import org.neo4j.driver.exceptions.DatabaseException
import org.neo4j.driver.internal.{InternalPath, InternalRelationship}
import org.neo4j.driver.{Value, Values}

import java.util
import java.util.{Collection, Map}
import scala.collection.JavaConverters._

object TypeConverter {

//  private def toNeoDirection(dir: Direction): org.neo4j.driver.types..graphdb.Direction = {
//    dir match {
//      case Direction.OUTGOING => org.neo4j.graphdb.Direction.OUTGOING
//      case Direction.BOTH => org.neo4j.graphdb.Direction.BOTH
//      case Direction.INCOMING => org.neo4j.graphdb.Direction.INCOMING
//    }
//  }
//
//  def toPandaDirection(dir: org.neo4j.graphdb.Direction): Direction = {
//    dir match {
//      case org.neo4j.graphdb.Direction.OUTGOING => Direction.OUTGOING
//      case org.neo4j.graphdb.Direction.BOTH => Direction.BOTH
//      case org.neo4j.graphdb.Direction.INCOMING => Direction.INCOMING
//    }
//  }
//
  def unwrapLynxValue(origin: LynxValue): Any = origin match {
    case l: LynxList => l.value.map(unwrapLynxValue)
    case m: LynxMap => m.value.mapValues(unwrapLynxValue)
    case n: LynxNode => lynxNode2NeoNode(n)
    case r: LynxRelationship => lynxRelationship2NeoRelationship(r)
    case p: LynxPath => lynxPath2NeoPath(p)
//      case p: Geographic2D => Values.ofPoint(org.neo4j.values.storable.CoordinateReferenceSystem.WGS84, p.x.value, p.y.value)
//      case p: Geographic3D => Values.pointValue(org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D, p.x.value, p.y.value, p.z.value)
//      case p: Cartesian2D => Values.pointValue(org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian, p.x.value, p.y.value)
//      case p: Cartesian3D => Values.pointValue(org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D, p.x.value, p.y.value, p.z.value)
    case other => other.value
  }

  def lynxNode2NeoNode(origin: LynxNode): org.neo4j.driver.types.Node = {
    val labels =  origin.labels.map(_.value).asJavaCollection
    new org.neo4j.driver.internal.InternalNode(origin.id.toLynxInteger.v, labels, getLynxElementProperties(origin))
  }

  def lynxRelationship2NeoRelationship(origin: LynxRelationship): org.neo4j.driver.types.Relationship = {
    new InternalRelationship(origin.id.toLynxInteger.v, origin.startNodeId.toLynxInteger.v, origin.endNodeId.toLynxInteger.v, origin.relationType.get.value, getLynxElementProperties(origin))
  }

  def lynxPath2NeoPath(origin: LynxPath): org.neo4j.driver.types.Path = {
    new InternalPath(origin.elements.map {
      case ln: LynxNode => lynxNode2NeoNode(ln)
      case lr: LynxRelationship => lynxRelationship2NeoRelationship(lr)
    }.asJava)
  }

  private def getLynxElementProperties(origin: LynxElement): util.Map[String, Value] = {
    origin.keys.map(k => k.value -> Values.value(unwrapLynxValue(origin.property(k).get))).toMap.asJava
  }

//  def toLynxValue(origin: Any): Any = {
//    origin match {
//      case v: Array[Any] => v.map(toLynxValue) // java array
//      case v: util.List[Any] => v.asScala.toList.map(toLynxValue)
//      case v: util.Map[String, Any] => v.asScala.toMap.mapValues(toLynxValue)
//      case v: util.Collection[Any] => v.asScala.map(toLynxValue)
//      case v: util.Set[Any] => v.asScala.toSet.map(toLynxValue)
//      case p: org.neo4j.driver.types.Point =>
//        val cs = p. .getCoordinate.getCoordinate
//        val x = LynxFloat(cs.get(0))
//        val y = LynxFloat(cs.get(1))
//        p.getCRS.getCode match {
//          case 4326 => Geographic2D(x, y)
//          case 4979 => Geographic3D(x, y, LynxFloat(cs.get(3)))
//          case 7203 => Cartesian2D(x, y)
//          case 9157 => Cartesian3D(x, y, LynxFloat(cs.get(3)))
//        }
//      case _: org.neo4j.graphdb.spatial.Geometry => throw new DatabaseException("PandaDB hasn't support Geometry data type.")
//      case v  => LynxValue(v)
//    }
//  }
}