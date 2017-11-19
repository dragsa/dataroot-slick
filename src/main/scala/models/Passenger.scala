package models

import slick.lifted.Tag
import slick.jdbc.PostgresProfile.api._
import scala.concurrent.Future

case class Passenger(passengerId: Int, passengerName: String)

final class PassengerTable(tag: Tag)
    extends Table[Passenger](tag, "passenger") {

  val passengerId = column[Int]("ID_psg", O.PrimaryKey)
  val passengerName = column[String]("name")

  def * =
    (passengerId, passengerName) <> (Passenger.apply _ tupled, Passenger.unapply)

}

object PassengerTable {
  val table = TableQuery[PassengerTable]
}

class PassengerRepository(implicit db: Database) {
  val passengerTableQuery = PassengerTable.table

  def createOne(passenger: Passenger): Future[Passenger] = {
    db.run(passengerTableQuery returning passengerTableQuery += passenger)
  }

  def createMany(passengers: List[Passenger]): Future[Seq[Passenger]] = {
    db.run(passengerTableQuery returning passengerTableQuery ++= passengers)
  }

  def updateOne(passenger: Passenger): Future[Int] = {
    db.run(
      passengerTableQuery
        .filter(_.passengerId === passenger.passengerId)
        .update(passenger))
  }

  def deleteOne(passengerId: Int): Future[Int] = {
    db.run(passengerTableQuery.filter(_.passengerId === passengerId).delete)
  }

  def getById(passengerId: Int): Future[Option[Passenger]] = {
    db.run(
      passengerTableQuery.filter(_.passengerId === passengerId).result.headOption)
  }
}