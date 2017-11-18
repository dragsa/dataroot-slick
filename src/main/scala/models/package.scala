import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

package object models {

  implicit def stringToTimestampConverter(str: String) = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSS")
    Timestamp.valueOf(LocalDateTime.parse(str, formatter))
  }

  implicit def timestampToStringConverter(stamp: Timestamp) = {
    stamp.toString
  }

  def inputDataConverterCompany(input: String) = {
    trimmer(input)
      .map { case List(x1, x2) => (x1, x2) }
      .map(a => Company.apply _ tupled ((Some(Integer.parseInt(a._1)), a._2)))
  }

  def inputDataConverterTrip(input: String) = {
    trimmer(input)
      .map {
        case List(x1, x2, x3, x4, x5, x6, x7) => (x1, x2, x3, x4, x5, x6, x7)
      }
      .map(
        a =>
          Trip.apply _ tupled ((Integer.parseInt(a._1),
                                Integer.parseInt(a._2),
                                a._3,
                                a._4,
                                a._5,
                                a._6,
                                a._7)))
  }

  def inputDataConverterPassenger(input: String) = {
    trimmer(input)
      .map {
        case List(x1, x2) => (x1, x2)
      }
      .map(a => Passenger.apply _ tupled ((Integer.parseInt(a._1), a._2)))
  }

  def inputDataConverterPassengerInTrip(input: String) = {
    trimmer(input)
      .map {
        case List(x1, x2, x3, x4) => (x1, x2, x3, x4)
      }
      .map(
        a =>
          PassengerInTrip.apply _ tupled ((Integer.parseInt(a._1),
                                           a._2,
                                           Integer.parseInt(a._3),
                                           a._4)))
  }

  def trimmer(str: String): List[List[String]] = {
    str
      .split("\n")
      .filter(_.nonEmpty)
      .map(_.trim.drop(1).dropRight(1))
      .map(_.split(",").toList.map(a => if (a.startsWith("\"")) a.replaceAll("\"", "") else a)).toList
  }
}
