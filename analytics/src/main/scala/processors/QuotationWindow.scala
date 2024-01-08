package processors

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import types.{CryptoQuotation, Quotation, StockQuotation}

import java.time.{Instant, OffsetDateTime, ZoneOffset}

class QuotationWindow[T <: Quotation] private (measurementType: WindowedVolumesMeasurement)
    extends WindowFunction[T, WindowedQuotationVolumes, String, TimeWindow] {
  override def apply(
      key: String,
      window: TimeWindow,
      input: Iterable[T],
      out: Collector[WindowedQuotationVolumes]
    ): Unit = {
    val sumBidVolume: BigDecimal = input.map(q => q.bid.price.amount * q.bid.lotSize).sum
    val sumAskVolume: BigDecimal = input.map(q => q.ask.price.amount * q.ask.lotSize).sum
    out.collect(
      new WindowedQuotationVolumes(
        measurementType,
        Map("symbol" -> key),
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(window.getEnd), ZoneOffset.UTC),
        sumBidVolume,
        sumAskVolume
      ))
  }
}

object QuotationWindow {

  def forStockQuotation(): QuotationWindow[StockQuotation] =
    new QuotationWindow[StockQuotation](WindowedStockQuotationVolumesMeasurement)

  def forCryptoQuotation(): QuotationWindow[CryptoQuotation] =
    new QuotationWindow[CryptoQuotation](WindowedCryptoQuotationVolumesMeasurement)
}
