package org.grapheco.pandadb.driver.neocompat
import org.neo4j.driver.{Bookmark, Query, Record, Result, Session, Transaction, TransactionConfig, TransactionWork, Value}

import org.grapheco.pandadb.client.PandaDBDriver
import java.util

case class SessionImpl(private val driver: PandaDBDriver) extends Session{ //TODO use real session

  private var _isOpen = true

  override def beginTransaction(): Transaction = TransactionImpl(driver.beginTransaction())

  override def beginTransaction(config: TransactionConfig): Transaction = beginTransaction

  override def readTransaction[T](work: TransactionWork[T]): T = {
    val tx = beginTransaction()
    val t = work.execute(tx)
    tx.commit()
    tx.close()
    t
  }

  override def readTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = readTransaction[T](work)

  override def writeTransaction[T](work: TransactionWork[T]): T = readTransaction[T](work) //TODO use real writeTransaction

  override def writeTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = readTransaction[T](work)

  override def run(query: String, config: TransactionConfig): Result = {
    val tx = beginTransaction(config)
    val r = tx.run(query)
    tx.commit()
    tx.close()
    r
  }

  override def run(query: String, parameters: util.Map[String, AnyRef], config: TransactionConfig): Result = {
    val tx = beginTransaction(config)
    val r = tx.run(query, parameters)
    tx.commit()
    tx.close()
    r
  }

  override def run(query: Query, config: TransactionConfig): Result = {
    val tx = beginTransaction(config)
    val r = tx.run(query)
    tx.commit()
    tx.close()
    r
  }

  override def lastBookmark(): Bookmark = ???

  override def reset(): Unit = ???

  override def close(): Unit = {_isOpen = false}

  override def run(query: String, parameters: Value): Result = {
    val tx = beginTransaction()
    val r = tx.run(query)
    tx.commit()
    tx.close()
    r
  }

  override def run(query: String, parameters: util.Map[String, AnyRef]): Result = {
    val tx = beginTransaction()
    val r = tx.run(query, parameters)
    tx.commit()
    tx.close()
    r
  }

  override def run(query: String, parameters: Record): Result = {
    val tx = beginTransaction()
    val r = tx.run(query, parameters)
    tx.commit()
    tx.close()
    r
  }

  override def run(query: String): Result = {
    val tx = beginTransaction()
    val r = tx.run(query)
    tx.commit()
    tx.close()
    r
  }

  override def run(query: Query): Result = {
    val tx = beginTransaction()
    val r = tx.run(query)
    tx.commit()
    tx.close()
    r
  }

  override def isOpen: Boolean = _isOpen
}
