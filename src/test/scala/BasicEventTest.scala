

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.MongoConnection
import com.novus.salat.Grater
import eventstore.BasicMongoStore
import org.scalatest.{BeforeAndAfter, FunSuite}




case class MyEvent(name:String)

case class MyEvent2(name:String)


case class Nougounou(name:String, ntype:String, power:Int = 100)

import BasicMongoStore._


object testBasicMongoStore extends BasicMongoStore( MongoConnection()("eventstore_test")("events_test"))   {
  def reset() {
    collection.remove(MongoDBObject())
  }
}

class BasicEventTest extends FunSuite with BeforeAndAfter {

  val store = testBasicMongoStore

  before {
    store.reset()
  }

  test("gratte") {
    val event: MyEvent = MyEvent("plouf")
    val asDBObject = implicitly[Grater[MyEvent]].asDBObject(event)
    import com.novus.salat._
    import com.mongodb.casbah.Imports._

    val graterOfLove = new ProxyGrater[AnyRef](classOf[AnyRef])
    graterOfLove.asObject(asDBObject)

  }


  test("basic storage") {
    store.appendEvent("master", MyEvent("ahoy"))

    store.appendEvent("master", MyEvent("plouf"))

    assert(store.readEvents("master").size == 2)
  }

  test("type info") {
    val event: MyEvent2 = MyEvent2("plouf")
    store.appendEvent("master", MyEvent("ahoy"))

    store.appendEvent("master", event)

    val payloads  = store.readEvents("master").map(_.payload).toList

    assert(payloads.size == 2)



    println(payloads)
    assert(payloads.collect({case e:MyEvent2 =>e} ).head ==  event)
  }

  test("version test") {
    assert(store.getVersion("master") == 0)

    store.appendEvent("master", MyEvent("plouf"))

    assert(store.getVersion("master") == 1)

    store.appendEvent("master", MyEvent("plouf2"))

    assert(store.getVersion("master") == 2)
  }




}