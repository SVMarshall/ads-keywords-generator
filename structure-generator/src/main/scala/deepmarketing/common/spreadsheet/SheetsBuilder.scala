package deepmarketing.common.spreadsheet

import java.io.FileNotFoundException

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.FileDataStoreFactory
import com.google.api.services.sheets.v4.{Sheets, SheetsScopes}
import com.google.common.base.Preconditions
import deepmarketing.common.Streams

import scala.collection.JavaConverters._

object SheetsBuilder {

  private case class ApiProperties(applicationName: String, secretFileResource: String, dataStorePath: String)

  private val apiProperties: ApiProperties = {
    // get apiProperties
    val prop = Streams.readProperties("spreadsheet-api.properties")

    Preconditions.checkNotNull(prop.getProperty("api.applicationName"))
    Preconditions.checkNotNull(prop.getProperty("api.serviceAccountPrivateKeyResource"))
    Preconditions.checkNotNull(prop.getProperty("api.dataStorePath"))

    ApiProperties(prop.getProperty("api.applicationName"),
                  prop.getProperty("api.serviceAccountPrivateKeyResource"),
                  prop.getProperty("api.dataStorePath"))
  }

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val datastoreFactory = new FileDataStoreFactory(new java.io.File(apiProperties.dataStorePath))
  private val jsonFactory = JacksonFactory.getDefaultInstance
  private val scopes = List(SheetsScopes.SPREADSHEETS_READONLY)

  def build(spreadSheetId: String, readOnly: Boolean = false): SpreadSheet = {
    new SpreadSheet(
      spreadSheetId,
      new Sheets.Builder(httpTransport, jsonFactory, getGoogleCredential(readOnly))
        .setApplicationName(apiProperties.applicationName)
        .build())
  }

  private def getGoogleCredential(readOnly: Boolean) = {
    val scopes: List[String] =
      if (readOnly) {
        List(SheetsScopes.SPREADSHEETS_READONLY, SheetsScopes.DRIVE_READONLY)
      } else {
        List(SheetsScopes.SPREADSHEETS, SheetsScopes.DRIVE)
      }

    try {
      GoogleCredential
        .fromStream(Streams.readResource(apiProperties.secretFileResource))
        .createScoped(scopes.asJavaCollection)
    } catch  {
      case e: NullPointerException =>
        throw new FileNotFoundException(
          String.format(
            "%s file with the Service Accounts Credentials must be set in your project. \n %s",
            apiProperties.secretFileResource, e))
    }
  }
}