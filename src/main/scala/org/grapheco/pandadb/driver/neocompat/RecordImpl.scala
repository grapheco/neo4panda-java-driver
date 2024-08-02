package org.grapheco.pandadb.driver.neocompat

import org.neo4j.driver.types.{Entity, Node, Path, Relationship}
import org.neo4j.driver.{Record, Value, Values}
import org.grapheco.lynx.LynxRecord
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.structural.{LynxNode, LynxPath, LynxRelationship}

import java.{lang, util}
import java.util.function
import scala.collection.JavaConverters._

case class RecordImpl(private val delegate: LynxRecord) extends Record {

  override def keys(): util.List[String] = delegate.cols.keys.toList.asJava

  override def values(): util.List[Value] = delegate.values.map(lv => Values.value(TypeConverter.unwrapLynxValue(lv))).toList.asJava

  override def index(key: String): Int = delegate.cols.getOrElse(key, throw new java.util.NoSuchElementException())

  override def get(index: Int): Value = delegate.get(index).map(lv => Values.value(TypeConverter.unwrapLynxValue(lv))).getOrElse(org.neo4j.driver.internal.value.NullValue.NULL)

  override def fields(): util.List[org.neo4j.driver.util.Pair[String, Value]] = {
    delegate.toMap.mapValues(lv => Values.value(TypeConverter.unwrapLynxValue(lv))).map(kv => org.neo4j.driver.internal.InternalPair.of(kv._1, kv._2)).toList.asJava
  }

  override def get(key: String, defaultValue: Value): Value = delegate.get(key).map(lv => Values.value(TypeConverter.unwrapLynxValue(lv))).getOrElse(defaultValue)

  override def get(key: String, defaultValue: Any): AnyRef = delegate.get(key).map(TypeConverter.unwrapLynxValue).getOrElse(defaultValue).asInstanceOf[AnyRef]

  override def get(key: String, defaultValue: Number): Number = delegate.get(key).map(lv => Values.value(TypeConverter.unwrapLynxValue(lv)).asInstanceOf[Number]).getOrElse(defaultValue)

  override def get(key: String, defaultValue: Entity): Entity = {
    delegate.get(key).map {
      case ln: LynxNode => TypeConverter.lynxNode2NeoNode(ln)
      case lr: LynxRelationship => TypeConverter.lynxRelationship2NeoRelationship(lr)
    }.getOrElse(defaultValue)
  }

  override def get(key: String, defaultValue: Node): Node = delegate.get(key).map(ln => TypeConverter.lynxNode2NeoNode(ln.asInstanceOf[LynxNode])).getOrElse(defaultValue)

  override def get(key: String, defaultValue: Path): Path = delegate.get(key).map(ln => TypeConverter.lynxPath2NeoPath(ln.asInstanceOf[LynxPath])).getOrElse(defaultValue)

  override def get(key: String, defaultValue: Relationship): Relationship = delegate.get(key).map(ln => TypeConverter.lynxRelationship2NeoRelationship(ln.asInstanceOf[LynxRelationship])).getOrElse(defaultValue)

  override def get(key: String, defaultValue: util.List[AnyRef]): util.List[AnyRef] = delegate.get(key).map(lv => TypeConverter.unwrapLynxValue(lv).asInstanceOf[List[AnyRef]].asJava).getOrElse(defaultValue)

  override def get[T](key: String, defaultValue: util.List[T], mapFunc: function.Function[Value, T]): util.List[T] = {
    delegate.get(key).map(lv =>
      TypeConverter.unwrapLynxValue(lv).asInstanceOf[List[AnyRef]].map(v => {
        mapFunc(Values.value(v))
      }).asJava
    ).getOrElse(defaultValue)
  }

  override def get(key: String, defaultValue: util.Map[String, AnyRef]): util.Map[String, AnyRef] = delegate.get(key).map(lv => TypeConverter.unwrapLynxValue(lv).asInstanceOf[Map[String, AnyRef]].asJava).getOrElse(defaultValue)

  override def get[T](key: String, defaultValue: util.Map[String, T], mapFunc: function.Function[Value, T]): util.Map[String, T] = {
    delegate.get(key).map(lv =>
      TypeConverter.unwrapLynxValue(lv).asInstanceOf[Map[String, AnyRef]].mapValues(v => {
        mapFunc(Values.value(v))
      }).asJava
    ).getOrElse(defaultValue)
  }

  override def get(key: String, defaultValue: Int): Int = delegate.get(key).map(lv => TypeConverter.unwrapLynxValue(lv).asInstanceOf[Int]).getOrElse(defaultValue)

  override def get(key: String, defaultValue: Long): Long = delegate.get(key).map(lv => TypeConverter.unwrapLynxValue(lv).asInstanceOf[Long]).getOrElse(defaultValue)

  override def get(key: String, defaultValue: Boolean): Boolean = delegate.get(key).map(lv => TypeConverter.unwrapLynxValue(lv).asInstanceOf[Boolean]).getOrElse(defaultValue)

  override def get(key: String, defaultValue: String): String = delegate.get(key).map(lv => TypeConverter.unwrapLynxValue(lv).asInstanceOf[String]).getOrElse(defaultValue)

  override def get(key: String, defaultValue: Float): Float = delegate.get(key).map(lv => TypeConverter.unwrapLynxValue(lv).asInstanceOf[Float]).getOrElse(defaultValue)

  override def get(key: String, defaultValue: Double): Double = delegate.get(key).map(lv => TypeConverter.unwrapLynxValue(lv).asInstanceOf[Double]).getOrElse(defaultValue)

  override def containsKey(key: String): Boolean = delegate.cols.contains(key)

  override def get(key: String): Value = delegate.get(key).map(lv => Values.value(TypeConverter.unwrapLynxValue(lv))).getOrElse(org.neo4j.driver.internal.value.NullValue.NULL)

  override def size(): Int = delegate.cols.size

  override def values[T](mapFunction: function.Function[Value, T]): lang.Iterable[T] = delegate.values.map(lv => mapFunction(Values.value(TypeConverter.unwrapLynxValue(lv)))).asJava

  override def asMap(): util.Map[String, AnyRef] = delegate.toMap.mapValues(_.value.asInstanceOf[AnyRef]).asJava

  override def asMap[T](mapFunction: function.Function[Value, T]): util.Map[String, T] = delegate.toMap.mapValues(lv => mapFunction(Values.value(TypeConverter.unwrapLynxValue(lv)))).asJava
}
