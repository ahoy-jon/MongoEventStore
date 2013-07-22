package eventstore

import com.mongodb.casbah.commons.MongoDBObject

import com.novus.salat._

import com.mongodb.casbah.Imports._
import scala.util.Try


case class DomainEvent(streamId: String, version: Int, payload: CaseClass)

trait EventStore {
    type DomainEventStream = Iterator[DomainEvent]

    def getVersion(streamId: String): Int

    def appendEvent[T <: CaseClass](streamId:String, event:T)(implicit grat:Grater[T]):Option[DomainEvent]

    def readEvents(streamId:String):DomainEventStream
}

object BasicMongoStore {
  
  val STREAM_ID = "streamId"
  val _ID = "_id"
  val VERSION = "version"
  val PAYLOAD = "payload"
  val _ID_STREAM_ID = dot(_ID,STREAM_ID)
  val _ID_VERSION = dot(_ID,VERSION)
  
  type $ = MongoDBObject
  val $ = MongoDBObject
  
  def dot(p1:String, p2:String):String = "%s.%s".format(p1,p2)

  implicit val ctx = new Context {
    val name = "Always"
    override val typeHintStrategy = StringTypeHintStrategy(when = TypeHintFrequency.Always)
  }

  implicit def grat[T<: CaseClass : Manifest] = grater[T]
}


class BasicMongoStore(protected val collection: MongoCollection) extends EventStore {
  import BasicMongoStore._

  def getVersion(streamId: String): Int = {

    val dbVersion:List[DBObject] = collection.find($(_ID_STREAM_ID -> streamId)).sort($(_ID_VERSION -> -1)).limit(1).toList

    val oVersion: Option[Int] = dbVersion.flatMap(_.getAs[DBObject](_ID).flatMap(_.getAs[Int](VERSION))).toList.headOption

    oVersion.getOrElse(0)
  }

  def appendEvent[T <: CaseClass](streamId: String, event: T)(implicit grat: Grater[T]):Option[DomainEvent] = {
    val nTry = 5
    val version = getVersion(streamId)

    def appendEventAttempt(offset: Int):Option[DomainEvent] = {
      val value = $(_ID -> $(STREAM_ID -> streamId, VERSION -> (version + 1 + offset)), PAYLOAD -> grat.asDBObject(event))
      try {
        val result = collection.insert(value, WriteConcern.FsyncSafe)
        Some(DomainEvent(streamId, (version + 1 + offset), event))
      } catch {
        case e: Exception => None
      }
    }

    (0 until nTry).toStream.flatMap(offset => appendEventAttempt(offset) ).headOption
  }

  def readEvents(streamId:String): DomainEventStream = {
    val graterOfLove = new ProxyGrater[AnyRef](classOf[AnyRef])(BasicMongoStore.ctx)

    collection.find($(_ID_STREAM_ID -> streamId)).toIterator.flatMap(dbo => {

      val asObject:Option[(String,Int)] = for {
        obj <- dbo.getAs[DBObject](_ID)
        streamId <-obj.getAs[String](STREAM_ID)
        version <- obj.getAs[Int](VERSION)
      } yield {
        streamId -> version
      }

      val asCaseClase:Option[CaseClass] = {
        for {
          dbobj <- dbo.getAs[DBObject](PAYLOAD)
          obj <- (Try {
            graterOfLove.asObject(dbobj).asInstanceOf[CaseClass]
          }).toOption
        } yield obj
      }

      for ( key <- asObject; cc <- asCaseClase) yield   DomainEvent(key._1, key._2, cc)
    })
  }
}