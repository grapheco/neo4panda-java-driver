package org.grapheco.pandadb.driver.neocompat

import org.grapheco.pandadb.client.PandaDBDriver
import org.neo4j.driver.async.AsyncSession
import org.neo4j.driver.reactive.RxSession
import org.neo4j.driver.{Driver, Metrics, Session, SessionConfig}
import org.neo4j.driver.types.TypeSystem

import java.lang
import java.util.concurrent.{CompletableFuture, CompletionStage}
case class DriverImpl(private val delegate: PandaDBDriver) extends Driver{

  override def isEncrypted: Boolean = false

  override def session(): Session = SessionImpl(delegate)

  override def session(sessionConfig: SessionConfig): Session = session()

  override def rxSession(): RxSession = ???

  override def rxSession(sessionConfig: SessionConfig): RxSession = ???

  override def asyncSession(): AsyncSession = ???

  override def asyncSession(sessionConfig: SessionConfig): AsyncSession = ???

  override def close(): Unit = delegate.shutdown()

  override def closeAsync(): CompletionStage[Void] = ???

  override def metrics(): Metrics = ???

  override def isMetricsEnabled: Boolean = false

  override def defaultTypeSystem(): TypeSystem = ???

  override def verifyConnectivity(): Unit = ???

  override def verifyConnectivityAsync(): CompletionStage[Void] = ???

  override def supportsMultiDb(): Boolean = false

  override def supportsMultiDbAsync(): CompletionStage[lang.Boolean] = CompletableFuture.completedFuture(false)
}
