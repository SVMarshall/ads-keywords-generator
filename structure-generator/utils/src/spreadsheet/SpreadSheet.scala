package spreadsheet

import java.util

import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.model._
import com.google.common.collect.Lists

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class SpreadSheet(spreadSheetId: String, private val sheetService: Sheets) {
  /**
    * Gets the content of the spreadsheet
    *
    * @param sheetName name of the sheet in the spreadsheet
    * @param range range of values to get with A1 notation (e.g., A1:G5)
    * @return list of rows
    */
  def getValues(sheetName: String, range: String): List[List[String]] = {
    sheetService
      .spreadsheets
      .values
      .get(spreadSheetId, s"$sheetName!$range")
      .execute
      .getValues
      // util.List to scala List
      .asScala.toList.map(_.asScala.toList.map(_.toString))
  }

  /**
    * Updates values on a spreadsheet (single range). If a range specifies a single cell,
    * the input data starts at that coordinate can extend any number of rows or columns
    *
    * @param sheetName name of the sheet in the spreadsheet
    * @param range range of values to get with A1 notation (e.g., A1:G5)
    * @param values list of rows to set
    */
  def updateValues(sheetName: String, range: String, values: List[List[Any]]): Unit = {
    val javaValues = toJavaValues(values)
    val body = new ValueRange().setValues(javaValues)
    val result = sheetService
      .spreadsheets
      .values
      .update(spreadSheetId, s"$sheetName!$range", body)
      .setValueInputOption("RAW").execute
  }

  private def toJavaValues(values: List[List[Any]]) = {
    val javaValues = Lists.newArrayList(
      values.map(row => {
        Lists.newArrayList(row.asJava)
          .asInstanceOf[util.List[AnyRef]]
      }).asJava)
      .asInstanceOf[util.List[util.List[AnyRef]]]
    javaValues
  }

  def addSheet(sheetName: String): Unit = {
    executeRequest(new Request()
                     .setAddSheet(new AddSheetRequest()
                                    .setProperties(new SheetProperties()
                                                     .setTitle(sheetName))))
  }

  private def executeRequest(request: Request): Unit = {
    sheetService
      .spreadsheets
      .batchUpdate(spreadSheetId,
                   new BatchUpdateSpreadsheetRequest()
                     .setRequests(List(request)))
      .execute()
  }

}
