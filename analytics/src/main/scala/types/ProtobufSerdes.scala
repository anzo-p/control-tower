package types

import market_data.crypto_quotation.CryptoQuotationProto
import market_data.crypto_trade.CryptoTradeProto
import market_data.market_data.MarketDataProto
import market_data.money.MoneyProto
import market_data.stock_quotation.StockQuotationProto
import market_data.stock_trade.StockTradeProto
import market_data.trade_unit.{CryptoTradeUnitProto, StockTradeUnitProto}
import types.TimeExtensions._

import java.time.OffsetDateTime

object ProtobufSerdes {

  private def fromProtobuf(proto: MoneyProto): Money =
    Money(
      amount   = proto.units + proto.nanos,
      currency = proto.currency
    )

  private def fromProtobuf(proto: CryptoTradeUnitProto): CryptoTradeUnit =
    CryptoTradeUnit(
      price   = fromProtobuf(proto.price.getOrElse(throw new IllegalArgumentException("Price data is required"))),
      lotSize = proto.lotSize
    )

  private def fromProtobuf(proto: StockTradeUnitProto): StockTradeUnit =
    StockTradeUnit(
      exchange = proto.exchange,
      price    = fromProtobuf(proto.price.getOrElse(throw new IllegalArgumentException("Price data is required"))),
      lotSize  = proto.lotSize
    )

  private def fromCryptoQuotationProtobuf(proto: CryptoQuotationProto, ingestTimestamp: OffsetDateTime): CryptoQuotation =
    CryptoQuotation(
      symbol = proto.symbol,
      bid    = fromProtobuf(proto.bid.getOrElse(throw new IllegalArgumentException("Bid data is required"))),
      ask    = fromProtobuf(proto.ask.getOrElse(throw new IllegalArgumentException("Ask data is required"))),
      marketTimestamp =
        proto.marketTimestamp.map(_.toJavaOffsetDateTime).getOrElse(throw new IllegalArgumentException("Market timestamp is required")),
      ingestTimestamp = ingestTimestamp
    )

  private def fromProtobuf(proto: CryptoTradeProto, ingestTimestamp: OffsetDateTime): CryptoTrade =
    CryptoTrade(
      symbol  = proto.symbol,
      tradeId = proto.tradeId,
      settle  = fromProtobuf(proto.settle.getOrElse(throw new IllegalArgumentException("Settle data is required"))),
      marketTimestamp =
        proto.marketTimestamp.map(_.toJavaOffsetDateTime).getOrElse(throw new IllegalArgumentException("Market timestamp is required")),
      ingestTimestamp = ingestTimestamp,
      tks             = proto.tks
    )

  private def fromProtobuf(proto: StockQuotationProto, ingestTimestamp: OffsetDateTime): StockQuotation =
    StockQuotation(
      symbol = proto.symbol,
      bid    = fromProtobuf(proto.bid.getOrElse(throw new IllegalArgumentException("Bid data is required"))),
      ask    = fromProtobuf(proto.ask.getOrElse(throw new IllegalArgumentException("Ask data is required"))),
      marketTimestamp =
        proto.marketTimestamp.map(_.toJavaOffsetDateTime).getOrElse(throw new IllegalArgumentException("Market timestamp is required")),
      ingestTimestamp = ingestTimestamp,
      conditions      = proto.conditions.toList,
      tape            = proto.tape
    )

  private def fromProtoBuf(proto: StockTradeProto, ingestTimestamp: OffsetDateTime): StockTrade =
    StockTrade(
      symbol  = proto.symbol,
      tradeId = proto.tradeId,
      settle  = fromProtobuf(proto.settle.getOrElse(throw new IllegalArgumentException("settle data is required"))),
      marketTimestamp =
        proto.marketTimestamp.map(_.toJavaOffsetDateTime).getOrElse(throw new IllegalArgumentException("Market timestamp is required")),
      ingestTimestamp = ingestTimestamp,
      conditions      = proto.conditions.toList,
      tape            = proto.tape
    )

  def fromProtoBuf(proto: MarketDataProto): MarketDataMessage =
    types.MarketDataMessage(
      ingestTimestamp =
        proto.ingestTimestamp.map(_.toJavaOffsetDateTime).getOrElse(throw new IllegalArgumentException("Ingest timestamp is required")),
      messageType = proto.messageType match {
        case MarketDataProto.MessageType.Cqm(message: CryptoQuotationProto) =>
          fromCryptoQuotationProtobuf(
            message,
            proto
              .ingestTimestamp
              .map(_.toJavaOffsetDateTime)
              .getOrElse(throw new IllegalArgumentException("Ingest timestamp is required for message type"))
          )

        case MarketDataProto.MessageType.Ctm(message: CryptoTradeProto) =>
          fromProtobuf(
            message,
            proto
              .ingestTimestamp
              .map(_.toJavaOffsetDateTime)
              .getOrElse(throw new IllegalArgumentException("Ingest timestamp is required for message type"))
          )

        case MarketDataProto.MessageType.Sqm(message: StockQuotationProto) =>
          fromProtobuf(
            message,
            proto
              .ingestTimestamp
              .map(_.toJavaOffsetDateTime)
              .getOrElse(throw new IllegalArgumentException("Ingest timestamp is required for message type"))
          )

        case MarketDataProto.MessageType.Stm(message: StockTradeProto) =>
          fromProtoBuf(
            message,
            proto
              .ingestTimestamp
              .map(_.toJavaOffsetDateTime)
              .getOrElse(throw new IllegalArgumentException("Ingest timestamp is required for message type"))
          )
      }
    )

  def toProtobuf(model: Money): MoneyProto = {
    MoneyProto(
      units    = model.amount.toLong,
      nanos    = (model.amount - model.amount.toLong).doubleValue(),
      currency = model.currency
    )
  }

  def toProtobuf(model: CryptoTradeUnit): CryptoTradeUnitProto =
    CryptoTradeUnitProto(
      price   = Some(toProtobuf(model.price)),
      lotSize = model.lotSize
    )

  def toProtobuf(model: StockTradeUnit): StockTradeUnitProto =
    StockTradeUnitProto(
      exchange = model.exchange,
      price    = Some(toProtobuf(model.price)),
      lotSize  = model.lotSize
    )

  def toProtobuf(model: CryptoQuotation): CryptoQuotationProto =
    CryptoQuotationProto(
      symbol          = model.symbol,
      bid             = Some(toProtobuf(model.bid)),
      ask             = Some(toProtobuf(model.ask)),
      marketTimestamp = Some(model.marketTimestamp.toProtobufTs)
    )

  def toProtobuf(model: CryptoTrade): CryptoTradeProto =
    CryptoTradeProto(
      symbol          = model.symbol,
      tradeId         = model.tradeId,
      settle          = Some(toProtobuf(model.settle)),
      marketTimestamp = Some(model.marketTimestamp.toProtobufTs),
      tks             = model.tks
    )

  def toProtobuf(model: StockQuotation): StockQuotationProto =
    StockQuotationProto(
      symbol          = model.symbol,
      bid             = Some(toProtobuf(model.bid)),
      ask             = Some(toProtobuf(model.ask)),
      marketTimestamp = Some(model.marketTimestamp.toProtobufTs),
      conditions      = model.conditions,
      tape            = model.tape
    )

  def toProtobuf(model: StockTrade): StockTradeProto =
    StockTradeProto(
      symbol          = model.symbol,
      tradeId         = model.tradeId,
      settle          = Some(toProtobuf(model.settle)),
      conditions      = model.conditions,
      marketTimestamp = Some(model.marketTimestamp.toProtobufTs),
      tape            = model.tape
    )
}
