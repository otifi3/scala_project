package engine

import scala.io.{BufferedSource, Source}
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.util.Using
import engine.AppLogger.logger

object rules extends App {

  try {
    logger.info("Starting order processing pipeline...")

    // Read CSV lines (skipping header)
    val lines: List[String] = Using.resource(Source.fromFile("src/main/resources/TRX1000.csv")) { source =>
      source.getLines().drop(1).toList
    }

    logger.info(s"Loaded ${lines.size} orders from CSV.")

    // -------------------------------Order class---------------------------
    case class Order(timestamp: String, product_name: String, expiry_date: String,
                     quantity: Int, unit_price: Double, channel: String, payment_method: String)

    // -------------------------------general functions--------------------------
    type BoolFunc = Order => Boolean
    type DiscountFunc = Order => Double

    val product_discountMap: Map[String, Double] = Map("cheese" -> 0.10, "wine" -> 0.05)
    val quantity_discountMap: Map[(Int, Int), Double] = Map(
      (6, 9) -> 0.05,
      (10, 14) -> 0.07,
      (15, Int.MaxValue) -> 0.10
    )

    def to_order(line: String): Order = {
      val parts = line.split(",")
      if (parts.length != 7)
        throw new IllegalArgumentException(s"Invalid line format: $line")
      Order(parts(0), parts(1), parts(2), parts(3).toInt,
        parts(4).toDouble, parts(5), parts(6))
    }

    def process_date(d: String): String = d.split('T')(0)
    def extract_day(d: String): Int = d.split('-')(2).toInt
    def extract_month(d: String): Int = d.split('-')(1).toInt
    def toDate(d: String): LocalDate = LocalDate.parse(d)
    def calc_days(startDate: LocalDate, endDate: LocalDate): Long =
      ChronoUnit.DAYS.between(startDate, endDate)

    // --------------------------Qualifying functions && rules------------------------------
    val is_less30: BoolFunc = order =>
      calc_days(toDate(process_date(order.timestamp)), toDate(order.expiry_date)) < 30

    val less30_Discount: DiscountFunc = order => {
      val rem_days = calc_days(toDate(process_date(order.timestamp)), toDate(order.expiry_date))
      (30 - rem_days) / 100.0
    }

    val is_chess_wine: BoolFunc = order => product_discountMap.contains(order.product_name)

    val chee_wine_Discount: DiscountFunc = order =>
      order.unit_price * product_discountMap.getOrElse(order.product_name, 0.0)

    val is_sold23March: BoolFunc = order => {
      val day = extract_day(process_date(order.timestamp))
      val month = extract_month(process_date(order.timestamp))
      day == 23 && month == 3
    }

    val sold23March_Discount: DiscountFunc = _ => 0.5

    val is_more5: BoolFunc = order => order.quantity > 5

    val more5_Discount: DiscountFunc = order => {
      val qty = order.quantity
      quantity_discountMap.collectFirst {
        case ((min, max), discount) if qty >= min && qty <= max => discount
      }.getOrElse(0.0)
    }

    val is_app: BoolFunc = order => order.channel == "App"

    val app_discount: DiscountFunc = order => {
      val roundedQuantity = ((order.quantity + 4) / 5) * 5
      (roundedQuantity / 5) * 5 / 100.0
    }

    val is_visa: BoolFunc = order => order.payment_method == "Visa"

    val visa_discount: DiscountFunc = _ => 0.5

    // ------------------------------list of rules-------------------------------
    def get_discountFunctions(): List[(BoolFunc, DiscountFunc)] = List(
      (is_less30, less30_Discount),
      (is_chess_wine, chee_wine_Discount),
      (is_sold23March, sold23March_Discount),
      (is_more5, more5_Discount),
      (is_app, app_discount),
      (is_visa, visa_discount)
    )

    val discountRules = get_discountFunctions()

    // ------------------------------processing orders---------------------------
    val processedOrders = lines.map(to_order).map { order =>
      val totalBefore = f"${order.unit_price * order.quantity}%.2f".toDouble

      val matchingDiscounts = discountRules.collect {
        case (cond, disc) if cond(order) => disc(order)
      }

      val topTwo = matchingDiscounts.sorted(Ordering[Double].reverse).take(2)
      val avgDiscount = if (topTwo.nonEmpty) topTwo.sum / topTwo.size else 0.0

      val totalAfter = f"${totalBefore - (totalBefore * avgDiscount)}%.2f".toDouble

      (order.product_name, totalBefore,
        f"${avgDiscount * 100}%.2f".toDouble, totalAfter)
    }

    // ------------------------------load to DB-------------------------------
    val insertSql =
      "INSERT INTO processed_orders_2 (product_name, total_before, discount, total_after) VALUES (?, ?, ?, ?)"

    try {
      Using.resource(db.connection) { conn =>
        Using.resource(conn.prepareStatement(insertSql)) { preparedStatement =>
          processedOrders.foreach { case (product, totalBefore, discount, totalAfter) =>
            preparedStatement.setString(1, product)
            preparedStatement.setDouble(2, totalBefore)
            preparedStatement.setDouble(3, discount)
            preparedStatement.setDouble(4, totalAfter)
            preparedStatement.executeUpdate()
          }
        }
      }
    } catch {
      case e: Exception =>
        logger.severe(s"Error inserting processed orders into database: ${e.getMessage}")
        throw e
    }

    logger.info("Order processing pipeline completed successfully.")

  } catch {
    case e: Exception =>
      logger.severe(s"Fatal error in order processing pipeline: ${e.getMessage}")
      throw e
  }
}
