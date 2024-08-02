package org.grapheco.pandadb.driver.neocompat
import org.neo4j.driver.{Record, Result}
import org.neo4j.driver.summary.ResultSummary

import java.util
import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.util.function
import java.util.function.Consumer

case class ResultCursorImpl(private val result: Result) extends org.neo4j.driver.async.ResultCursor{

  override def keys(): util.List[String] = result.keys()

  override def consumeAsync(): CompletionStage[ResultSummary] = ???

  override def nextAsync(): CompletionStage[Record] = CompletableFuture.completedFuture(result.next())

  override def peekAsync(): CompletionStage[Record] = CompletableFuture.completedFuture(result.peek())

  override def singleAsync(): CompletionStage[Record] = CompletableFuture.completedFuture(result.single())

  override def forEachAsync(action: Consumer[Record]): CompletionStage[ResultSummary] = ???

  override def listAsync(): CompletionStage[util.List[Record]] = CompletableFuture.completedFuture(result.list())

  override def listAsync[T](mapFunction: function.Function[Record, T]): CompletionStage[util.List[T]] = CompletableFuture.completedFuture(result.list[T](mapFunction))
}
