package processors

import java.time.OffsetDateTime

trait InfluxdbResult {
  def measurement: WindowedVolumesMeasurement
  def tags: String
  def fields: String
  def toLineProtocol: String
}

abstract class WindowResult extends InfluxdbResult {
  def windowEndTime: OffsetDateTime

  private def escapes(str: String): String = {
    str.replaceAll("([ ,=])", "\\\\$1")
  }

  override def toLineProtocol: String = {
    val timestamp = windowEndTime.toInstant.getEpochSecond * 1000000000L
    s"${measurement.value},$tags $fields $timestamp"
  }
}

case class WindowedQuotationVolumes(
    measurementType: WindowedVolumesMeasurement,
    tagBy: Map[String, String],
    windowEndTime: OffsetDateTime,
    sumBidVolume: BigDecimal,
    sumAskVolume: BigDecimal
  ) extends WindowResult {
  override val measurement  = measurementType
  override val tags: String = tagBy.map { case (k, v) => s"$k=$v" }.mkString(",")
  override val fields       = s"sumBidVolume=$sumBidVolume,sumAskVolume=$sumAskVolume"
}
