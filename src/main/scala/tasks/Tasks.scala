package tasks

import scala.concurrent.Await
import scala.concurrent.duration._
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

  def query72 = {
    val subQuery72 =
      (repoTrip.tripTableQuery join repoTrip.passInTripTableQuery on (_.tripNumber === _.tripNumber))
        .map { case (t, p) => (t.companyId, p.passengerId) }
        .sortBy(a => (a._2.desc, a._1.desc))
        .groupBy(companyPassToken => companyPassToken._2)
        .map {
          case (key, rows) =>
            (key, rows.length, rows.map(_._1).avg, rows.map(_._1).max)
        }
        .filter {
          case (_, _, avgCompany, maxCompany) => avgCompany === maxCompany
        }
        .map { case (pass, tripsNum, _, _) => (pass, tripsNum) }
        .sortBy { case (pass, tripsNum) => (pass.desc, tripsNum.desc) }
    (for {
      p <- repoPass.passengerTableQuery
      luckyPassenger <- subQuery72 if p.passengerId === luckyPassenger._1
    } yield (p, luckyPassenger))
      .map { case (pass, subSelect) => (pass.passengerName, subSelect._2) }
  }

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

  // TODO
  def query79 = {
//    (repoTrip.passInTripTableQuery join repoTrip.tripTableQuery on (_.tripNumber === _.tripNumber))
//      .groupBy { case (pit, t) => pit.passengerId }
//      .map { case (key, rows) => key -> rows.map(row => row._2.timeIn.asInstanceOf[Int] -- row._2.timeOut.asInstanceOf[Int]).max }

  }

  // TODO
  def query84 = {
    ???
  }

//  // for 72, special treatment
//  println({
//    val result = exec(query72.result)
//    val resultDecapitated = result.head
//    result.takeWhile(a => a._2 == resultDecapitated._2)
//  }.foreach(println(_)))

//  // for 77, special treatment
//  println({
//    val result = exec(query77.result)
//    val resultDecapitated = result.head
//    result.takeWhile(a => a._1 == resultDecapitated._1)
//  }.foreach(println(_)))

  println(exec(query77.result).foreach(println(_)))
}
