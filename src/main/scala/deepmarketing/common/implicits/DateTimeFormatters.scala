package deepmarketing.common.implicits

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object DateTimeFormatters {

  implicit class DateTimeWithFormatters(dateTime: DateTime) {
    def toTimestamp: String = dateTime.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss"))
    def toDate: String = dateTime.toString(DateTimeFormat.forPattern("yyyyMMdd"))
    def toTime: String = dateTime.toString(DateTimeFormat.forPattern("HH:mm:ss"))
    def toDateTime: String = dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
  }
}