package appconfig

import helpers.Extensions.EnvOps

import java.net.URL

case class InfluxDetails(uri: URL, token: String)

object InfluxDetails {

  def make(): InfluxDetails = {
    val baseUrl   = sys.env.getOrThrow("INFLUXDB_URL", "INFLUXDB_URL is not set")
    val org       = sys.env.getOrThrow("INFLUXDB_ORG", "INFLUXDB_ORG is not set")
    val token     = sys.env.getOrThrow("INFLUXDB_TOKEN", "INFLUXDB_TOKEN is not set").replace("\"", "")
    val uriPrefix = s"$baseUrl/api/v2/write?org=$org"

    InfluxDetails(new URL(uriPrefix), token)
  }
}
