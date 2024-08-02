package org.grapheco.pandadb.driver.neocompat
import org.neo4j.driver.{Query, Record, Value, Transaction}
import org.neo4j.driver.async.{AsyncTransaction, ResultCursor}

import java.util
import java.util.concurrent.{CompletionStage, CompletableFuture}

case class AsyncTransactionImpl(private val delegate: Transaction) extends AsyncTransaction {

  override def commitAsync(): CompletionStage[Void] = CompletableFuture.completedFuture{
    delegate.commit()
    null
  }

  override def rollbackAsync(): CompletionStage[Void] = CompletableFuture.completedFuture{
    delegate.rollback()
    null
  }

  override def closeAsync(): CompletionStage[Void] = CompletableFuture.completedFuture{
    delegate.close()
    null
  }

  override def runAsync(query: String, parameters: Value): CompletionStage[ResultCursor] = CompletableFuture.completedFuture(ResultCursorImpl(delegate.run(query, parameters)))

  override def runAsync(query: String, parameters: util.Map[String, AnyRef]): CompletionStage[ResultCursor] = CompletableFuture.completedFuture(ResultCursorImpl(delegate.run(query, parameters)))

  override def runAsync(query: String, parameters: Record): CompletionStage[ResultCursor] = CompletableFuture.completedFuture(ResultCursorImpl(delegate.run(query, parameters)))

  override def runAsync(query: String): CompletionStage[ResultCursor] = CompletableFuture.completedFuture(ResultCursorImpl(delegate.run(query)))

  override def runAsync(query: Query): CompletionStage[ResultCursor] = CompletableFuture.completedFuture(ResultCursorImpl(delegate.run(query)))
}
