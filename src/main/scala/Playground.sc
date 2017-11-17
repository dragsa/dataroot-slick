import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import models.Company

val str = "19000101 01:00:00.000"
val formatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSS")
val dateTime = Timestamp.valueOf(LocalDateTime.parse(str, formatter))

val companines = """(1,"Don_avia")
  (2,"Aeroflot")
  (3,"Dale_avia")
  (4,"air_France")
  (5,"British_AW")""".split("\n").filter(_.nonEmpty).map(_.trim.drop(1).dropRight(1)).map(a => {
  val arrayOf = a.split(",")
  arrayOf.toList
}).map { case List(x1, x2) => (x1, x2) }
  .map(a => Company.apply _ tupled ((Some(Integer.parseInt(a._1)), a._2)))