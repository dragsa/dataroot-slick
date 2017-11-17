import scala.concurrent.Await
import scala.concurrent.duration._
import slick.jdbc.PostgresProfile.api._
import models._

object EntryPoint {

  implicit val db = Database.forURL(
    "jdbc:postgresql://localhost:5432/airport?user=postgres&password=postgres")

  val companyRepository = new CompanyRepository
  val tripRepository = new TripRepository
  val passengerRepository = new PassengerRepository

  val companies = inputDataConverterCompany(InputData.COMPANIES)
  val passengers = inputDataConverterPassenger(InputData.PASSENGERS)
  val trips = inputDataConverterTrip(InputData.TRIPS)
  val passInTrips = inputDataConverterPassengerInTrip(InputData.PASSENGERS_IN_TIP)

  def initTable(): Unit = {
    Await.result(db.run(CompanyTable.table.schema.create), Duration.Inf)
    Await.result(db.run(TripTable.table.schema.create), Duration.Inf)
    Await.result(db.run(PassengerTable.table.schema.create), Duration.Inf)
    Await.result(db.run(PassengerInTripTable.table.schema.create), Duration.Inf)
  }

  def fillTablesWithData(): Unit = {
    val companiesExisting = Await.result(companyRepository.createMany(companies), Duration.Inf)
    val passengersExisting = Await.result(passengerRepository.createMany(passengers), Duration.Inf)
    Await.result(tripRepository.createMany(trips, companiesExisting.map(_.companyId.get), passengersExisting.map(_.passengerId), passInTrips), Duration.Inf)
  }

  def main(args: Array[String]): Unit = {
    initTable()
    fillTablesWithData()

  }
}
