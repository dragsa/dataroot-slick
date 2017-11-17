package models

import java.sql.Timestamp
import slick.lifted.Tag
import slick.jdbc.PostgresProfile.api._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class Trip(tripNumber: Int,
                companyId: Int,
                plane: String,
                townFrom: String,
                townTo: String,
                timeOut: Timestamp,
                timeIn: Timestamp)

final class TripTable(tag: Tag) extends Table[Trip](tag, "trip") {

  val tripNumber = column[Int]("trip_no", O.PrimaryKey)
  val companyId = column[Int]("ID_comp")
  val plane = column[String]("plane")
  // TODO from <> to constraint here?
  val townFrom = column[String]("town_from")
  val townTo = column[String]("town_to")
  val timeOut = column[Timestamp]("time_out")
  val timeIn = column[Timestamp]("time_in")

  val companyFk =
    foreignKey("ID_comp_fk", companyId, TableQuery[CompanyTable])(_.companyId)

  def * =
    (tripNumber, companyId, plane, townFrom, townTo, timeOut, timeIn) <> (Trip.apply _ tupled, Trip.unapply)

}

object TripTable {
  val table = TableQuery[TripTable]
}

case class PassengerInTrip(tripNumber: Int,
                           date: Timestamp,
                           passengerId: Int,
                           place: String)

final class PassengerInTripTable(tag: Tag)
    extends Table[PassengerInTrip](tag, "pass_in_trip") {

  val tripNumber = column[Int]("trip_no")
  val date = column[Timestamp]("date")
  val passengerId = column[Int]("ID_psg")
  val place = column[String]("place")

  val tripNoFk =
    foreignKey("trip_no_fk", tripNumber, TableQuery[TripTable])(_.tripNumber)
  val passengerIdFk =
    foreignKey("psg_id_fk", passengerId, TableQuery[PassengerTable])(
      _.passengerId)

  val pk = primaryKey("pass_in_trip_pk", (tripNumber, date, passengerId))

  def * =
    (tripNumber, date, passengerId, place) <> (PassengerInTrip.apply _ tupled, PassengerInTrip.unapply)
}

object PassengerInTripTable {
  val table = TableQuery[PassengerInTripTable]
}

class TripRepository(implicit db: Database) {
  val tripTableQuery = TripTable.table
  val passInTripTableQuery = PassengerInTripTable.table

  def createOne(trip: Trip): Future[Trip] = {
    db.run(tripTableQuery returning tripTableQuery += trip)
  }

  def createMany(trips: List[Trip],
                 companiesIds: Seq[Int],
                 passengerIds: Seq[Int],
                 passInTrips: List[PassengerInTrip]): Future[Seq[Trip]] = {
    val query = (tripTableQuery returning tripTableQuery ++= trips.filter(a =>
      companiesIds.contains(a.companyId)))
      .flatMap { insertedTrips =>
        (passInTripTableQuery ++= passInTrips.filter(
          a =>
            insertedTrips.toList
              .map(trip => trip.tripNumber)
              .contains(a.tripNumber) &&
              passengerIds.contains(a.passengerId)))
          .andThen(DBIO.successful(insertedTrips))
      }
    db.run(query)
  }

  def update(trip: Trip): Future[Int] = {
    db.run(tripTableQuery.filter(_.tripNumber === trip.tripNumber).update(trip))
  }

  def delete(tripNumber: Int): Future[Int] = {
    db.run(tripTableQuery.filter(_.tripNumber === tripNumber).delete)
  }

  def getById(tripNumber: Int): Future[Option[Trip]] = {
    db.run(tripTableQuery.filter(_.tripNumber === tripNumber).result.headOption)
  }
}
