package models

import slick.lifted.Tag
import slick.jdbc.PostgresProfile.api._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class Company(companyId: Option[Int] = None, name: String)

final class CompanyTable(tag: Tag) extends Table[Company](tag, "company") {

  val companyId = column[Int]("ID_comp", O.PrimaryKey, O.AutoInc)
  val name = column[String]("name")

  def * =
    (companyId.?, name) <> (Company.apply _ tupled, Company.unapply)

}

object CompanyTable {
  val table = TableQuery[CompanyTable]
}

class CompanyRepository(implicit db: Database) {
  val companyTableQuery = CompanyTable.table

  def createOne(company: Company): Future[Company] = {
    db.run(companyTableQuery returning companyTableQuery += company)
  }

  def createMany(companies: List[Company]): Future[Seq[Company]] = {
    db.run(companyTableQuery returning companyTableQuery ++= companies)
  }

  def update(company: Company): Future[Int] = {
    db.run(
      companyTableQuery
        .filter(_.companyId === company.companyId)
        .update(company))
  }

  def delete(companyId: Int): Future[Int] = {
    db.run(companyTableQuery.filter(_.companyId === companyId).delete)
  }

  def getById(companyId: Int): Future[Option[Company]] = {
    db.run(
      companyTableQuery.filter(_.companyId === companyId).result.headOption)
  }
}
