package org.grapheco.pandadb.driver.neocompat
import org.neo4j.driver.{Bookmark, Query, Record, TransactionConfig, Value, Session}
import org.neo4j.driver.async.{AsyncSession, AsyncTransaction, AsyncTransactionWork, ResultCursor}

import java.util
import java.util.concurrent.{CompletionStage, CompletableFuture}

case class AsyncSessionImpl(private val delegate: Session) extends AsyncSession {

  override def beginTransactionAsync(): CompletionStage[AsyncTransaction] = CompletableFuture.completedFuture(AsyncTransactionImpl(delegate.beginTransaction()))

  override def beginTransactionAsync(config: TransactionConfig): CompletionStage[AsyncTransaction] = CompletableFuture.completedFuture(AsyncTransactionImpl(delegate.beginTransaction(config)))

  override def readTransactionAsync[T](work: AsyncTransactionWork[CompletionStage[T]]): CompletionStage[T] = {
    val asyncTx = AsyncTransactionImpl(delegate.beginTransaction())
    val result = work.execute(asyncTx)
    CompletableFuture.completedFuture(result).thenCompose(t => {
      asyncTx.commitAsync().thenCompose((v) => asyncTx.closeAsync())
      t
    })
  }

  override def readTransactionAsync[T](work: AsyncTransactionWork[CompletionStage[T]], config: TransactionConfig): CompletionStage[T] = readTransactionAsync[T](work)

  override def writeTransactionAsync[T](work: AsyncTransactionWork[CompletionStage[T]]): CompletionStage[T] = readTransactionAsync[T](work)

  override def writeTransactionAsync[T](work: AsyncTransactionWork[CompletionStage[T]], config: TransactionConfig): CompletionStage[T] = readTransactionAsync[T](work)

  override def runAsync(query: String, config: TransactionConfig): CompletionStage[ResultCursor] = CompletableFuture.completedFuture(ResultCursorImpl(delegate.run(query)))

  override def runAsync(query: String, parameters: util.Map[String, AnyRef], config: TransactionConfig): CompletionStage[ResultCursor] = CompletableFuture.completedFuture(ResultCursorImpl(delegate.run(query, parameters)))

  override def runAsync(query: Query, config: TransactionConfig): CompletionStage[ResultCursor] = CompletableFuture.completedFuture(ResultCursorImpl(delegate.run(query, config)))

  override def lastBookmark(): Bookmark = ???

  override def closeAsync(): CompletionStage[Void] = {
    CompletableFuture.completedFuture{
      delegate.close()
      null
    }
  }

  override def runAsync(query: String, parameters: Value): CompletionStage[ResultCursor] = CompletableFuture.completedFuture(ResultCursorImpl(delegate.run(query, parameters)))

  override def runAsync(query: String, parameters: util.Map[String, AnyRef]): CompletionStage[ResultCursor] = CompletableFuture.completedFuture(ResultCursorImpl(delegate.run(query, parameters)))

  override def runAsync(query: String, parameters: Record): CompletionStage[ResultCursor] = CompletableFuture.completedFuture(ResultCursorImpl(delegate.run(query, parameters)))

  override def runAsync(query: String): CompletionStage[ResultCursor] = CompletableFuture.completedFuture(ResultCursorImpl(delegate.run(query)))

  override def runAsync(query: Query): CompletionStage[ResultCursor] = CompletableFuture.completedFuture(ResultCursorImpl(delegate.run(query)))
}
