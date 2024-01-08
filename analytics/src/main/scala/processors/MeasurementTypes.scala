package processors

sealed trait WindowedVolumesMeasurement {
  def value: String
}

case object WindowedStockQuotationVolumesMeasurement extends WindowedVolumesMeasurement {
  override val value = "windowed-stock-quotation-volumes"
}

case object WindowedCryptoQuotationVolumesMeasurement extends WindowedVolumesMeasurement {
  override val value = "windowed-crypto-quotation-volumes"
}
