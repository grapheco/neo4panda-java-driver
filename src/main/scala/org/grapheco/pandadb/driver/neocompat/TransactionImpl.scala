package org.grapheco.pandadb.driver.neocompat

import org.neo4j.driver._

import java.util
import org.grapheco.pandadb.client.{Transaction => PandaTx}
import org.grapheco.pandadb.util.Logging
import org.grapheco.lynx.LynxResult
import org.neo4j.driver.exceptions.ClientException

import scala.collection.JavaConverters._

case class TransactionImpl(private val delegate: PandaTx) extends Transaction with Logging {

  private var txState = TxState.STARTED

  override def commit(): Unit = {
    if (txState == TxState.Terminated) return rollback()
    txState = TxState.Committed
    delegate.commit()
  }

  override def rollback(): Unit = {
    txState = TxState.Rollbacked
    delegate.rollback()
  }

  override def close(): Unit = {
    if (txState != TxState.Committed) {
      rollback()
      delegate.close()
    }
    txState = TxState.Closed
    //TODO do we need manually call rollback???
  }

  override def run(query: String, parameters: util.Map[String, AnyRef]): Result = {
    checkState()
    val lr = delegate.executeQuery(query, parameters.asScala.toMap)
    ResultImpl(lr)
  }

  override def run(query: String, parameters: Value): Result = run(query, parameters.asMap())

  override def run(query: String, parameters: Record): Result = run(query, parameters.asMap())

  override def run(query: String): Result = {
    checkState()
    var lr: LynxResult = null
    try {
      lr = delegate.executeQuery(query)
    } catch {
      case e: Exception => throw e
    } finally {
      close()
    }
    ResultImpl(lr)
  }

  override def run(query: Query): Result = run(query.text(), query.parameters())

  override def isOpen = txState != TxState.Closed

  private def checkState(): Unit = {
    if (txState == TxState.Terminated) throw new ClientException("Transaction terminated")
    if (txState == TxState.Committed) throw new ClientException("Transaction committed")
    if (txState == TxState.Closed) throw new ClientException("Transaction closed")
  }
}
object TxState extends Enumeration {
  val STARTED, Terminated, Rollbacked, Committed, Closed = Value
}