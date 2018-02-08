package com.ovoenergy.bootcamp.kafka.service.acquisition

import java.lang

import akka.http.scaladsl.client.RequestBuilding
import com.ovoenergy.bootcamp.kafka.common.Randoms
import com.ovoenergy.bootcamp.kafka.domain.{Arbitraries, CreateAcquisition}
import com.ovoenergy.comms.dockertestkit.{KafkaKit, ZookeeperKit}
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import com.whisk.docker.scalatest.DockerTestKit
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.concurrent.Eventually
import org.scalatest.{FlatSpec, Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.concurrent.Future

class KafkaSpec extends BaseIntegrationSpec with RequestBuilding with FailFastCirceSupport with Arbitraries with Randoms
  with Eventually {

  type Key = String
  type Value = String

  val topic = "acquisitions"

  var producer: Producer[Key,Value] = _
  var consumer: Consumer[Key, Value] = _
  var adminClient: AdminClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    consumer = new KafkaConsumer[Key, Value](
      Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "127.0.0.1:9092",
        ConsumerConfig.GROUP_ID_CONFIG -> "my-consumer-group-id",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
        ConsumerConfig.CLIENT_ID_CONFIG -> "KafkaConsumerSpec"
      ).asJava,
      new StringDeserializer,
      new StringDeserializer
    )
  }

  override def afterAll(): Unit = {
    producer.close()
    consumer.close()
    adminClient.close()
    super.afterAll()
  }

  "produce a kafka record" in {
    val ca = random[CreateAcquisition]
    whenReady(createAcquisition(ca)){ acquisition =>

      acquisition.customerName shouldBe ca.customerName

      consumer.subscribe(List(topic).asJava)

      eventually {
        consumer.poll(1000).count() shouldBe 1
      }
    }
  }

//  "consumer" should {
//    "consume the produced messages" in {
//
//      adminClient.createTopics(List(new NewTopic(topic, 6, 1)).asJava).all().get()
//
//      Future.sequence((0 to 9).map(i =>
//        produceRecord(producer, new ProducerRecord[Key, Value](topic, s"$i", s"test-$i"))
//      )).futureValue(timeout(5.seconds))
//
//      consumer.subscribe(Set(topic).asJava)
//
//      val noOfRecordsConsumed = Iterator
//        .continually(consumer.poll(150))
//        .map { records =>
//          records.asScala.foreach(processRecord)
//          consumer.commitSync()
//          records.count()
//        }
//        .take(10)
//        .sum
//
//      noOfRecordsConsumed shouldBe 10
//
//    }
//  }

}
