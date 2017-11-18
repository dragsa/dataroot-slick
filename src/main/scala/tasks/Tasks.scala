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
    val subQuery67 = repoTrip.tripTableQuery
      .groupBy(t => (t.townTo, t.townFrom))
      .map { case (key, rows) => (rows.length, key._1, key._2) }
      .groupBy(tShort => tShort._1)
      .map { case (key, rows) => key -> rows.length }
      .sortBy(row => row._1.desc)
      .take(1)
      .map(_._2)
    subQuery67
  }

  println(exec(query67.result))
}
