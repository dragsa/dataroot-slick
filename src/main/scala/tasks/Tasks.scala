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
    ???
  }

  def query84 = {
    val firstDecade = (stringToTimestampConverter("20030401 00:00:00.000"),
                       stringToTimestampConverter("20030410 23:59:00.000"))
    val secondDecade = (stringToTimestampConverter("20030411 00:00:00.000"),
                        stringToTimestampConverter("20030420 23:59:00.000"))
    val thirdDecade = (stringToTimestampConverter("20030421 00:00:00.000"),
                       stringToTimestampConverter("20030430 23:59:00.000"))

    (for {
      pit <- repoTrip.passInTripTableQuery
      t <- repoTrip.tripTableQuery if pit.tripNumber === t.tripNumber
    } yield
      (t.companyId,
       Case
         .If(pit.date >= firstDecade._1 && pit.date <= firstDecade._2)
         .Then(1)
         .Else(0),
       Case
         .If(pit.date >= secondDecade._1 && pit.date <= secondDecade._2)
         .Then(1)
         .Else(0),
       Case
         .If(pit.date >= thirdDecade._1 && pit.date <= thirdDecade._2)
         .Then(1)
         .Else(0)))
      .filter(a => (a._2 + a._3 + a._4) =!= 0)
      .groupBy { case (company, _, _, _) => company }
      .map {
        case (key, rows) =>
          (key, rows.map(_._2).sum, rows.map(_._3).sum, rows.map(_._4).sum)
      }
      .sortBy(_._1)
  }

  def query88 = {
    val passAsIdWithTripNumsAndCompanyId =
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
        .map {
          case (pass, tripsNum, companyId, _) => (pass, tripsNum, companyId)
        }

    val passWithTripNumsAndCompanyId = (for {
      p <- repoPass.passengerTableQuery
      luckyPassenger <- passAsIdWithTripNumsAndCompanyId
    } yield (p, luckyPassenger))
      .filter {
        case (p, lp) => p.passengerId === lp._1
      }
      .map {
        case (pass, subSelect) =>
          (pass.passengerName, subSelect._2, subSelect._3)
      }

    // weird thing here - exception saying that column X doesn't exist.
    // perhaps to much columns in resulting select. moving to for comprehension in above.
//    val passWithTripNumsAndCompanyId = (repoPass.passengerTableQuery join passAsIdWithTripNumsAndCompanyId on (_.passengerId === _._1))
//      .map { case (pass, subSelect) => (pass.passengerName, subSelect._2, subSelect._3) }

    (repoCompany.companyTableQuery join passWithTripNumsAndCompanyId on (_.companyId === _._3))
      .map {
        case (company, subSelect) => (subSelect._1, subSelect._2, company.name)
      }
      .sortBy { case (_, tripsNum, _) => tripsNum.desc }
  }

  def query95 = {
    val tripPassJoin = (for {
      t <- repoTrip.tripTableQuery
      pit <- repoTrip.passInTripTableQuery if t.tripNumber === pit.tripNumber
    } yield (t, pit))
      .map {
        case (t, pit) =>
          (t.companyId, t.plane, pit.tripNumber, pit.date, pit.passengerId)
      }

    val flights = tripPassJoin
      .groupBy { case (id, _, trip, date, _) => (id, trip, date) }
      .map { case (key, _) => key }
      .groupBy { case (id, _, _) => id }
      .map { case (key, rows) => key -> rows.length }

    val diffPlanes = tripPassJoin
      .groupBy { case (id, plane, _, _, _) => (id, plane) }
      .map { case (key, _) => key }
      .groupBy { case (id, _) => id }
      .map { case (key, rows) => key -> rows.length }

    val diffPassengers = tripPassJoin
      .groupBy { case (id, _, _, _, pass) => (id, pass) }
      .map { case (key, _) => key }
      .groupBy { case (id, _) => id }
      .map { case (key, rows) => key -> rows.length }

    val totalPassengers = tripPassJoin
      .groupBy { case (id, _, trip, date, _) => (id, trip, date) }
      .map { case (key, rows) => (key._1,rows.length) }
      .groupBy { case (id, _) => id }
      .map { case (key, rows) => key -> rows.map(_._2).sum }

    (for {
      c <- repoCompany.companyTableQuery
      f <- flights if f._1 === c.companyId
      p <- diffPlanes if p._1 === c.companyId
      dp <- diffPassengers if dp._1 === c.companyId
      tp <- totalPassengers if tp._1 === c.companyId
    } yield (c.companyId, f._2, p._2, dp._2, tp._2))
      .sortBy(_._1)

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

//  // for 88, special treatment
//  println({
//    val result = exec(query88.result)
//    val resultDecapitated = result.head
//    result.takeWhile(a => a._2 == resultDecapitated._2)
//  }.foreach(println(_)))

  println(exec(query95.result).foreach(println(_)))
}
