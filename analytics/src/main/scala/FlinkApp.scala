import appconfig.{InfluxDetails, StreamConfig}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import processors.{QuotationWindow, StreamHelpers, WindowedQuotationVolumes}
import sinks.ResultSink
import types._

import java.time.{Duration => JavaDuration}

object FlinkApp {

  private def flinkStream(influxDetails: InfluxDetails): Unit = {
    val env = StreamConfig.createExecutionEnvironment()

    val marketDataStream: DataStream[MarketDataMessage] =
      env.addSource(StreamConfig.buildConsumer())

    val stockQuotationStream: DataStream[StockQuotation]   = StreamHelpers.filterType[StockQuotation](marketDataStream)
    val cryptoQuotationStream: DataStream[CryptoQuotation] = StreamHelpers.filterType[CryptoQuotation](marketDataStream)

    val watermarkedStockQuotationStream: DataStream[StockQuotation] = StreamHelpers.watermarkForBound(
      stockQuotationStream,
      dueTime      = JavaDuration.ofSeconds(180),
      idlePatience = JavaDuration.ofSeconds(30)
    )

    val watermarkedCryptoQuotationStream: DataStream[CryptoQuotation] = StreamHelpers.watermarkForBound(
      cryptoQuotationStream,
      dueTime      = JavaDuration.ofSeconds(180),
      idlePatience = JavaDuration.ofSeconds(30)
    )

    val windowedStockQuotationVolumes: DataStream[WindowedQuotationVolumes] = watermarkedStockQuotationStream
      .keyBy(_.symbol)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(60), Time.seconds(30)))
      .apply(QuotationWindow.forStockQuotation())

    val windowedCryptoQuotationVolumes: DataStream[WindowedQuotationVolumes] = watermarkedCryptoQuotationStream
      .keyBy(_.symbol)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(60), Time.seconds(30)))
      .apply(QuotationWindow.forCryptoQuotation())

    windowedStockQuotationVolumes.addSink(ResultSink.forStockQuotation(influxDetails))
    windowedCryptoQuotationVolumes.addSink(ResultSink.forCryptoQuotation(influxDetails))

    env.execute("Flink Kinesis Example")
  }

  def main(args: Array[String]): Unit = {
    StreamHelpers.checkInfluxDB()
    flinkStream(InfluxDetails.make())
  }
}
