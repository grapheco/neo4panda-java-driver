package org.grapheco.pandadb.driver.neocompat

import org.neo4j.driver.{Record, Result}
import org.neo4j.driver.exceptions.NoSuchRecordException
import org.neo4j.driver.summary.ResultSummary
import org.grapheco.lynx.{LynxRecord, LynxResult}

import scala.collection.JavaConverters._
import java.util
import java.util.{Spliterator, Spliterators}
import java.util.function.Function
import java.util.stream.{Stream, StreamSupport}


case class ResultImpl(private val delegate: LynxResult) extends Result {

  private val _iterator: Iterator[LynxRecord] = delegate.records()
  private val _columns: util.List[String] = delegate.columns().toList.asJava
  private var _prefetchedRecord: Record = null

  /**
   * Retrieve the keys of the records this result contains.
   *
   * @return all keys
   */
  override def keys: util.List[String] = _columns

  /**
   * Investigate the next upcoming record without moving forward in the result.
   *
   * @return the next record
   * @throws NoSuchRecordException if there is no record left in the stream
   */
  override def peek: Record = {
    if (_prefetchedRecord != null) return _prefetchedRecord
    if (!this._iterator.hasNext) return null
    val re = RecordImpl(_iterator.next)
    if (re == null) return null
    _prefetchedRecord = re
    re
  }

  /**
   * Test if there is another record we can navigate to in this result.
   *
   * @return true if {@link # next ( )} will return another record
   */
  override def hasNext: Boolean = {
    if (peek() != null) {
      return true
    }
    false
  }

  /**
   * Navigate to and retrieve the next {@link Record} in this result.
   *
   * @return the next record
   * @throws NoSuchRecordException if there is no record left in the stream
   */
  override def next: Record = {
    if (_prefetchedRecord != null) {
      val ret = _prefetchedRecord
      _prefetchedRecord = null
      return ret
    }
    throw new NoSuchRecordException("") //TODO really need?
  }

  /**
   * Return the first record in the result, failing if there is not exactly
   * one record left in the stream
   * <p>
   * Calling this method always exhausts the result, even when {@link NoSuchRecordException} is thrown.
   *
   * @return the first and only record in the stream
   * @throws NoSuchRecordException if there is not exactly one record left in the stream
   */
  @throws[NoSuchRecordException]
  override def single: Record = {
    if (!hasNext) throw new NoSuchRecordException("")
    val ret = next
    if (hasNext) throw new NoSuchRecordException("")
    ret
  }

  /**
   * Convert this result to a sequential {@link Stream} of records.
   * <p>
   * Result is exhausted when a terminal operation on the returned stream is executed.
   *
   * @return sequential {@link Stream} of records. Empty stream if this result has already been consumed or is empty.
   */
  override def stream: Stream[Record] = {
    val spliterator = Spliterators.spliteratorUnknownSize(this, Spliterator.IMMUTABLE | Spliterator.ORDERED)
    StreamSupport.stream(spliterator, false)
  }

  /**
   * Retrieve and store the entire result stream.
   * This can be used if you want to iterate over the stream multiple times or to store the
   * whole result for later use.
   * <p>
   * Note that this method can only be used if you know that the query that
   * yielded this result returns a finite stream. Some queries can yield
   * infinite results, in which case calling this method will lead to running
   * out of memory.
   * <p>
   * Calling this method exhausts the result.
   *
   * @return list of all remaining immutable records
   */
  override def list: util.List[Record] = {
    val l = new util.LinkedList[Record]
    while (hasNext) l.add(next)
    l
  }

  /**
   * Retrieve and store a projection of the entire result.
   * This can be used if you want to iterate over the stream multiple times or to store the
   * whole result for later use.
   * <p>
   * Note that this method can only be used if you know that the query that
   * yielded this result returns a finite stream. Some queries can yield
   * infinite results, in which case calling this method will lead to running
   * out of memory.
   * <p>
   * Calling this method exhausts the result.
   *
   * @param mapFunction a function to map from Record to T. See {@link Records} for some predefined functions.
   * @return list of all mapped remaining immutable records
   */
  override def list[T](mapFunction: Function[Record, T]): util.List[T] = {
    val l = new util.LinkedList[T]
    while (hasNext) l.add(mapFunction.apply(next))
    l
  }

  /**
   * Return the result summary.
   * <p>
   * If the records in the result is not fully consumed, then calling this method will exhausts the result.
   * <p>
   * If you want to access unconsumed records after summary, you shall use {@link Result# list ( )} to buffer all records into memory before summary.
   *
   * @return a summary for the whole query result.
   */
  override def consume: ResultSummary = ???
}
