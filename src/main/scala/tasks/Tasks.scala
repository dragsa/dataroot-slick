package tasks

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import models._
import slick.jdbc.PostgresProfile.api._

object Tasks extends App {
  implicit val db = Database.forURL(
    "jdbc:postgresql://localhost:5432/airport?user=postgres&password=postgres")

  val repoTrip = new TripRepository()
  val repoPass = new PassengerRepository()
  val repoCompany = new CompanyRepository()

  def exec[T](action: DBIO[T]): T = Await.result(db.run(action), 2.seconds)

  def query63 = {
    val subQuery63 = repoTrip.passInTripTableQuery
      .groupBy(pit => (pit.passengerId, pit.place))
      .map { case (key, rows) => key._1 -> rows.length }
      .filter(_._2 >= 2)
      .map { case (id, _) => id }
    repoPass.passengerTableQuery
      .filter(_.passengerId in subQuery63)
      .map(_.passengerName)
  }

  def query67 = {
    val query67 = repoTrip.tripTableQuery
      .groupBy(t => (t.townTo, t.townFrom))
      .map { case (key, rows) => (rows.length, key._1, key._2) }
      .groupBy(tShort => tShort._1)
      .map { case (key, rows) => key -> rows.length }
      .sortBy(row => row._1.desc)
      .take(1)
      .map(_._2)
    query67
  }

  // TODO
  def query68 = {
    ???
  }

  // TODO
  def query72 = {
    val subQuery72 =
      (repoTrip.tripTableQuery join repoTrip.passInTripTableQuery on (_.tripNumber === _.tripNumber))
        .map { case (t, p) => (t.companyId, p.passengerId) }
        .groupBy(companyPassToken => (companyPassToken._1, companyPassToken._2))
        .map { case (key, rows) => (key._1, key._2) -> rows.length }
        .sortBy(a => (a._1._1.desc, a._1._2.desc))
    println(subQuery72.result.statements.mkString)
    subQuery72
  }

  // TODO
  // confusing, what should we return if there are several max days?
  def query77 = {
    (for {
      pit <- repoTrip.passInTripTableQuery
        .map(a => (a.tripNumber, a.date))
        .groupBy(x => x)
        .map(_._1)
      t <- repoTrip.tripTableQuery if pit._1 === t.tripNumber
    } yield (pit, t))
      .filter { case (_, t) => t.townFrom === "Rostov" }
      .groupBy { case (pit, _) => pit._2 }
      .map { case (key, rows) => rows.length -> key }
      .sortBy(a => (a._1.desc, a._2.asc))
  }

  def query79 = {
    (repoTrip.passInTripTableQuery join repoTrip.tripTableQuery on (_.tripNumber === _.tripNumber))
      .groupBy{ case (pit, t) => pit.passengerId}
      .map { case (key, rows) => key -> rows.map(row => 4).sum}

  }

  println(exec(query72.result).foreach(println(_)))
}
