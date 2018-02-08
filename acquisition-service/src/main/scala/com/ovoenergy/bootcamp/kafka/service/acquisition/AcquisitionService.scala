package com.ovoenergy.bootcamp.kafka.service.acquisition

import java.util.concurrent.{ConcurrentHashMap}

import akka.http.scaladsl.model.ContentTypes.`text/plain(UTF-8)`
import akka.http.scaladsl.model.{HttpEntity, StatusCodes}
import akka.http.scaladsl.server.{Directives, PathMatcher1, Route}
import buildinfo.BuildInfo
import com.ovoenergy.bootcamp.kafka.common.serde._
import com.ovoenergy.bootcamp.kafka.domain.Acquisition.AcquisitionId
import com.ovoenergy.bootcamp.kafka.domain.{CreateAcquisition, _}
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.apache.kafka.clients.producer.RecordMetadata

import scala.collection.JavaConverters._
import scala.concurrent.Future

object AcquisitionService
    extends Directives
    with ErrorAccumulatingCirceSupport {

  type AcquisitionRepository = ConcurrentHashMap[AcquisitionId, Acquisition]

  val acquisitionIdMatcher: PathMatcher1[AcquisitionId] =
    Segment.map(AcquisitionId.apply)

  implicit val buildInfoEncoder: Encoder[BuildInfo.type] = Encoder.instance {
    bi =>
      Json.obj(
        "name" -> bi.name.asJson,
        "version" -> bi.version.asJson
      )
  }

  def routes(acquisitionRepository: AcquisitionRepository,
             produceAcquisition: Acquisition => Future[Unit]): Route = {
    extractMaterializer { implicit m =>
      extractActorSystem { implicit s =>
        extractExecutionContext { implicit e =>
          path("admin" / "ping") {
            complete(HttpEntity(`text/plain(UTF-8)`, "pong"))
          } ~
            path("admin" / "info") {
              complete(BuildInfo)
            } ~
            pathPrefix("api" / "v1") {
              pathPrefix("acquisition") {
                pathEndOrSingleSlash {
                  get {
                    complete(acquisitionRepository.values.asScala)
                  } ~
                    (post & entity(as[CreateAcquisition])) {
                      createAcquisition: CreateAcquisition =>
                        val acquisitionId = AcquisitionId.unique()
                        val acquisition: Acquisition = createAcquisition.toAcquisition(acquisitionId)


                        complete(produceAcquisition(acquisition).map{_ =>
                          acquisitionRepository.put(acquisitionId, acquisition)
                          acquisition
                        })
                    }
                } ~
                  (path(acquisitionIdMatcher) & get) { acquisitionId =>
                    Option(acquisitionRepository.get(acquisitionId))
                      .fold(complete(StatusCodes.NotFound))(complete(_))
                  }
              }
            }
        }
      }
    }
  }

}
