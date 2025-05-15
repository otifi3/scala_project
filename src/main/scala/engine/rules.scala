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

    // Order case class
    case class Order(timestamp: String,
                      product_name: String,
                      expiry_date: String,
                      quantity: Int,
                      unit_price: Double,
                      channel: String,
                      payment_method: String
                    )

    // Function types for readability
    type BoolFunc = Order => Boolean
    type DiscountFunc = Order => Double

    // Discount Maps
    val productDiscountMap: Map[String, Double] = Map("cheese" -> 0.10, "wine" -> 0.05)
    val quantityDiscountMap: Map[(Int, Int), Double] = Map(
      (6, 9) -> 0.05,
      (10, 14) -> 0.07,
      (15, Int.MaxValue) -> 0.10
    )

    // Parsing CSV line into Order
    def toOrder(line: String): Order = {
      val parts = line.split(",")
      if (parts.length != 7)
        throw new IllegalArgumentException(s"Invalid line format: $line")
      Order(
        timestamp = parts(0),
        product_name = parts(1),
        expiry_date = parts(2),
        quantity = parts(3).toInt,
        unit_price = parts(4).toDouble,
        channel = parts(5),
        payment_method = parts(6)
      )
    }

    // Date helper functions
    def processDate(d: String): String = d.split('T')(0)
    def extractDay(d: String): Int = d.split('-')(2).toInt
    def extractMonth(d: String): Int = d.split('-')(1).toInt
    def toDate(d: String): LocalDate = LocalDate.parse(d)
    def calcDays(startDate: LocalDate, endDate: LocalDate): Long =
      ChronoUnit.DAYS.between(startDate, endDate)

    // Qualifying predicates and discount functions
    val isLessThan30Days: BoolFunc = order =>
      calcDays(toDate(processDate(order.timestamp)), toDate(order.expiry_date)) < 30

    val lessThan30Discount: DiscountFunc = order => {
      val remDays = calcDays(toDate(processDate(order.timestamp)), toDate(order.expiry_date))
      (30 - remDays) / 100.0
    }

    val isCheeseOrWine: BoolFunc = order => productDiscountMap.contains(order.product_name)

    val cheeseWineDiscount: DiscountFunc = order =>
      order.unit_price * productDiscountMap.getOrElse(order.product_name, 0.0)

    val isSoldOnMarch23: BoolFunc = order => {
      val day = extractDay(processDate(order.timestamp))
      val month = extractMonth(processDate(order.timestamp))
      day == 23 && month == 3
    }

    val soldMarch23Discount: DiscountFunc = _ => 0.5

    val isQuantityMoreThan5: BoolFunc = order => order.quantity > 5

    val quantityDiscount: DiscountFunc = order => {
      val qty = order.quantity
      quantityDiscountMap.collectFirst {
        case ((min, max), discount) if qty >= min && qty <= max => discount
      }.getOrElse(0.0)
    }

    val isSoldViaApp: BoolFunc = order => order.channel == "App"

    val appDiscount: DiscountFunc = order => {
      val roundedQuantity = ((order.quantity + 4) / 5) * 5
      (roundedQuantity / 5) * 5 / 100.0
    }

    val isVisaPayment: BoolFunc = order => order.payment_method == "Visa"

    val visaDiscount: DiscountFunc = _ => 0.5

    // List of discount rules as pairs of predicate and discount calculator
    val discountRules: List[(BoolFunc, DiscountFunc)] = List(
      (isLessThan30Days, lessThan30Discount),
      (isCheeseOrWine, cheeseWineDiscount),
      (isSoldOnMarch23, soldMarch23Discount),
      (isQuantityMoreThan5, quantityDiscount),
      (isSoldViaApp, appDiscount),
      (isVisaPayment, visaDiscount)
    )

    // Process orders: parse, calculate discounts and final prices
    val processedOrders: List[(String, Double, Double, Double)] = lines.map(toOrder).map { order =>
      val totalBefore = BigDecimal(order.unit_price * order.quantity).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

      val applicableDiscounts = discountRules.collect {
        case (cond, disc) if cond(order) => disc(order)
      }

      val topTwoDiscounts = applicableDiscounts.sorted(Ordering[Double].reverse).take(2)
      val avgDiscount = if (topTwoDiscounts.nonEmpty) topTwoDiscounts.sum / topTwoDiscounts.size else 0.0

      val totalAfter = BigDecimal(totalBefore * (1 - avgDiscount)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

      (order.product_name,
        totalBefore,
        BigDecimal(avgDiscount * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
        totalAfter)
    }

    // Database insertion SQL statements
    val insertSql_2 =
      "INSERT INTO processed_orders_2 (product_name, total_before, discount, total_after) VALUES (?, ?, ?, ?)"

    // Insert processed orders into DB
    Using.resource(db.connection) { conn =>
      Using.resource(conn.prepareStatement(insertSql_2)) { preparedStatement =>
        processedOrders.foreach { case (product, totalBefore, discount, totalAfter) =>
          preparedStatement.setString(1, product)
          preparedStatement.setDouble(2, totalBefore)
          preparedStatement.setDouble(3, discount)
          preparedStatement.setDouble(4, totalAfter)
          preparedStatement.executeUpdate()
        }
      }
    }

    logger.info("Order processing pipeline completed successfully.")

  } catch {
    case e: Exception =>
      logger.severe(s"Fatal error in order processing pipeline: ${e.getMessage}")
      throw e
  }
}
