package com.ovoenergy.bootcamp.kafka.service.acquisition

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.{HttpApp, Route}
import akka.stream.{ActorMaterializer, Materializer}
import ciris.syntax._
import ciris.{env, loadConfig, prop}
import com.ovoenergy.bootcamp.kafka.service.acquisition.AcquisitionService.AcquisitionRepository
import com.ovoenergy.bootcamp.kafka.common.config._
import com.ovoenergy.bootcamp.kafka.domain.{Acquisition, CreateAcquisition}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration
import scala.io.StdIn
import scala.util.{Failure, Success}
import scala.collection.JavaConverters._
import scala.concurrent.Future

object Main extends App {

  val log = LoggerFactory.getLogger(getClass)

  type Key = String
  type Value = String
  def kafkaEndpointHC: String = s"127.0.0.1:9092"

  val producer = new KafkaProducer[Key, Value](
    Map[String, AnyRef](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaEndpointHC,
      ProducerConfig.ACKS_CONFIG->"all",
      ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION->"1",
      ProducerConfig.CLIENT_ID_CONFIG -> "KafkaConsumerSpec"
    ).asJava,
    new StringSerializer,
    new StringSerializer
  )

  val acquisitionTopicName = "acquisitions"

  def produceAcquisition(acq: Acquisition): Future[Unit] = {
    val record = new ProducerRecord[String, String]("acquisition", acq.id.value, acq.id.value)
    produceRecord[String, String](producer, record).map(_ => ())
  }
//
//  val produceAcquisition: Acquisition => Future[RecordMetadata] = (acquisition: Acquisition) =>
//    produceRecord(producer, new ProducerRecord[String, String](
//      acquisitionTopicName,
//      acquisition.id.value,
//      acquisition.toString)
//    )

  def produceRecord[K,V](producer: Producer[K, V], record: ProducerRecord[K, V]): Future[RecordMetadata] = {
    val p = Promise[RecordMetadata]
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        Option(exception) match {
          case Some(e) => p.failure(e)
          case None => p.success(metadata)
        }
      }
    })
    p.future
  }

  val settings: Settings = loadConfig(
    env[Option[String]]("HTTP_HOST").orElse(prop[Option[String]]("http.host")),
    env[Option[Int]]("HTTP_PORT").orElse(prop[Option[Int]]("http.port")),
    env[String]("KAFKA_ENDPOINT").orElse(prop[String]("kafka.endpoint"))
  )(
    (host, port, kafkaEndpoint) =>
      Settings(host.getOrElse("0.0.0.0"),
        port.getOrElse(8081),
        kafkaEndpointHC)).orThrow()

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  val routes: Route = AcquisitionService.routes(
    new AcquisitionRepository,
    produceAcquisition
  )

  val bindingFuture = Http().bindAndHandle(routes, settings.httpHost, settings.httpPort)

  bindingFuture.onComplete {
    case Success(binding) =>
      log.info(s"Server online at http://${binding.localAddress.getHostName}:${binding.localAddress.getPort}/")
      sys.addShutdownHook(Await.result(system.terminate(), Duration.Inf))

    case Failure(e) =>
      log.error("Error binding the server", e)
      sys.exit(1)
  }
}
