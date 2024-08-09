package org.grapheco.pandadb.driver.neocompat
import org.neo4j.driver.{Bookmark, Query, Record, Result, Session, Transaction, TransactionConfig, TransactionWork, Value}

import scala.collection.JavaConverters._
import org.grapheco.pandadb.client.{Session => PSession}
import org.grapheco.pandadb.util.Logging

import java.util

case class SessionImpl(private val delegate: PSession) extends Session  with Logging {

  private var _isOpen = true

  override def beginTransaction(): Transaction = TransactionImpl(delegate.beginTransaction())

  override def beginTransaction(config: TransactionConfig): Transaction = beginTransaction

  override def readTransaction[T](work: TransactionWork[T]): T = {
    val tx = beginTransaction()
    var t: Any = null
    try{
      t = work.execute(tx)
      tx.commit()
    } catch {
      case e: Exception => throw e
    } finally {
      tx.close()
    }
    t.asInstanceOf[T]
  }

  override def readTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = readTransaction[T](work)

  override def writeTransaction[T](work: TransactionWork[T]): T = readTransaction[T](work) //TODO use real writeTransaction

  override def writeTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = readTransaction[T](work)

  override def run(query: String, config: TransactionConfig): Result = ResultImpl(delegate.run(query))

  override def run(query: String, parameters: util.Map[String, AnyRef], config: TransactionConfig): Result = ResultImpl(delegate.run(query, parameters.asScala.toMap))

  override def run(query: Query, config: TransactionConfig): Result = ResultImpl(delegate.run(query.text(), query.parameters().asMap().asScala.toMap))

  override def lastBookmark(): Bookmark = ???

  override def reset(): Unit = ???

  override def close(): Unit = {
    _isOpen = false
    delegate.close()
  }

  override def run(query: String, parameters: Value): Result = ResultImpl(delegate.run(query, parameters.asMap().asScala.toMap))

  override def run(query: String, parameters: util.Map[String, AnyRef]): Result = ResultImpl(delegate.run(query, parameters.asScala.toMap))

  override def run(query: String, parameters: Record): Result = ResultImpl(delegate.run(query, parameters.asMap().asScala.toMap))

  override def run(query: String): Result = ResultImpl(delegate.run(query))

  override def run(query: Query): Result = ResultImpl(delegate.run(query.text(), query.parameters().asMap().asScala.toMap))

  override def isOpen: Boolean = _isOpen
}
