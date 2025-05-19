package engine

import scala.io.{BufferedSource, Source}
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.util.Using
import engine.AppLogger.logger

/**
 * Object `rules` implements an order processing pipeline that:
 * - Reads raw order data from a CSV file.
 * - Parses each CSV line into an Order case class.
 * - Defines a set of discount rules with qualifying conditions and discount calculations.
 * - Processes each order to calculate total price before discount,
 *   determines applicable discounts, averages the top two discounts,
 *   and calculates the final price after discounts.
 * - Inserts the processed order results into a database table.
 * - Logs key pipeline stages and handles errors gracefully.
 *
 * The pipeline steps:
 * 1. Read input CSV `TRX1000.csv` and skip the header.
 * 2. Define Order class representing the input data schema.
 * 3. Define various discount rules, each with:
 *    - A Boolean predicate function to check if an order qualifies.
 *    - A discount function returning the discount amount.
 * 4. Parse each line to an Order object.
 * 5. For each order:
 *    - Calculate total price before discount.
 *    - Collect all discounts that apply to the order.
 *    - Pick the top two highest discounts and average them.
 *    - Calculate the final total price after applying the average discount.
 * 6. Insert processed order data (product name, totals, discount) into DB.
 * 7. Log successes and errors.
 *
 * Usage notes:
 * - CSV input format must have exactly 7 comma-separated columns:
 *   timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method
 * - Discount rules include product-based, quantity-based, date-based, sales channel, and payment method.
 * - Discount values are expressed as doubles (e.g., 0.05 for 5%).
 * - Date calculations use java.time.LocalDate and ChronoUnit.DAYS.
 * - Database connection `db.connection` is assumed available externally.
 * - Uses Scala's `Using` for safe resource management.
 */
object rules extends App {

  try {
    logger.info("Starting order processing pipeline...")

    /**
     * Reads CSV lines from file, skipping the header line.
     * @return List of raw CSV lines (String) excluding the header.
     */
    val lines: List[String] = Using.resource(Source.fromFile("src/main/resources/TRX1000.csv")) { source =>
      source.getLines().drop(1).toList
    }

    logger.info(s"Loaded ${lines.size} orders from CSV.")

    /**
     * Case class representing an order's data.
     * @param timestamp    Timestamp of order placement.
     * @param product_name Product name string.
     * @param expiry_date  Expiry date string (ISO format).
     * @param quantity     Number of units ordered.
     * @param unit_price   Price per unit.
     * @param channel      Sales channel (e.g., App, Store).
     * @param payment_method Payment method (e.g., Visa).
     */
    case class Order(timestamp: String, product_name: String, expiry_date: String,
                     quantity: Int, unit_price: Double, channel: String, payment_method: String)

    /** 
     * Type alias for qualifying condition function for discount rules.
     * Takes an Order and returns Boolean indicating if discount applies.
     */
    type BoolFunc = Order => Boolean

    /**
     * Type alias for discount calculation function.
     * Takes an Order and returns a Double discount value.
     */
    type DiscountFunc = Order => Double

    /** 
     * Map of product-based discounts for specific products.
     */
    val product_discountMap: Map[String, Double] = Map("cheese" -> 0.10, "wine" -> 0.05)

    /** 
     * Map of quantity ranges to discounts.
     * Each key is a tuple (minQuantity, maxQuantity).
     */
    val quantity_discountMap: Map[(Int, Int), Double] = Map(
      (6, 9) -> 0.05,
      (10, 14) -> 0.07,
      (15, Int.MaxValue) -> 0.10
    )

    /**
     * Parses a CSV line into an Order instance.
     * @param line CSV line string.
     * @throws IllegalArgumentException if line does not have exactly 7 columns.
     * @return Order instance.
     */
    def to_order(line: String): Order = {
      val parts = line.split(",")
      if (parts.length != 7)
        throw new IllegalArgumentException(s"Invalid line format: $line")
      Order(parts(0), parts(1), parts(2), parts(3).toInt,
        parts(4).toDouble, parts(5), parts(6))
    }

    /**
     * Extracts date portion (yyyy-MM-dd) from ISO timestamp string.
     * @param d Timestamp string in ISO format.
     * @return Date portion string.
     */
    def process_date(d: String): String = d.split('T')(0)

    /**
     * Extracts day as integer from date string (yyyy-MM-dd).
     * @param d Date string.
     * @return Day of month integer.
     */
    def extract_day(d: String): Int = d.split('-')(2).toInt

    /**
     * Extracts month as integer from date string (yyyy-MM-dd).
     * @param d Date string.
     * @return Month integer.
     */
    def extract_month(d: String): Int = d.split('-')(1).toInt

    /**
     * Parses date string into LocalDate.
     * @param d Date string.
     * @return LocalDate object.
     */
    def toDate(d: String): LocalDate = LocalDate.parse(d)

    /**
     * Calculates number of days between two dates.
     * @param startDate Start LocalDate.
     * @param endDate   End LocalDate.
     * @return Days between startDate and endDate as Long.
     */
    def calc_days(startDate: LocalDate, endDate: LocalDate): Long =
      ChronoUnit.DAYS.between(startDate, endDate)

    // -------------------------- Discount qualifying and calculation functions ------------------------------

    /**
     * Condition: Order expiry date is less than 30 days after order timestamp.
     */
    val is_less30: BoolFunc = order =>
      calc_days(toDate(process_date(order.timestamp)), toDate(order.expiry_date)) < 30

    /**
     * Discount based on how close to expiry the order is, scaled between 0 and 0.30.
     */
    val less30_Discount: DiscountFunc = order => {
      val rem_days = calc_days(toDate(process_date(order.timestamp)), toDate(order.expiry_date))
      (30 - rem_days) / 100.0
    }

    /**
     * Condition: Order product is in product_discountMap (e.g., cheese or wine).
     */
    val is_chess_wine: BoolFunc = order => product_discountMap.contains(order.product_name)

    /**
     * Discount amount for cheese or wine products based on unit price.
     */
    val chee_wine_Discount: DiscountFunc = order =>
      order.unit_price * product_discountMap.getOrElse(order.product_name, 0.0)

    /**
     * Condition: Order was sold specifically on March 23rd.
     */
    val is_sold23March: BoolFunc = order => {
      val day = extract_day(process_date(order.timestamp))
      val month = extract_month(process_date(order.timestamp))
      day == 23 && month == 3
    }

    /**
     * Fixed 50% discount if order sold on March 23rd.
     */
    val sold23March_Discount: DiscountFunc = _ => 0.5

    /**
     * Condition: Order quantity greater than 5.
     */
    val is_more5: BoolFunc = order => order.quantity > 5

    /**
     * Discount based on quantity ranges.
     */
    val more5_Discount: DiscountFunc = order => {
      val qty = order.quantity
      quantity_discountMap.collectFirst {
        case ((min, max), discount) if qty >= min && qty <= max => discount
      }.getOrElse(0.0)
    }

    /**
     * Condition: Order placed through the "App" channel.
     */
    val is_app: BoolFunc = order => order.channel == "App"

    /**
     * Discount based on app orders rounded quantity tiers.
     */
    val app_discount: DiscountFunc = order => {
      val roundedQuantity = ((order.quantity + 4) / 5) * 5
      (roundedQuantity / 5) * 5 / 100.0
    }

    /**
     * Condition: Payment method is Visa.
     */
    val is_visa: BoolFunc = order => order.payment_method == "Visa"

    /**
     * Fixed 5% discount for Visa payment method.
     */
    val visa_discount: DiscountFunc = _ => 0.05

    /**
     * Returns a list of tuples pairing qualifying conditions with discount functions.
     * @return List of (BoolFunc, DiscountFunc) tuples representing discount rules.
     */
    def get_discountFunctions(): List[(BoolFunc, DiscountFunc)] = List(
      (is_less30, less30_Discount),
      (is_chess_wine, chee_wine_Discount),
      (is_sold23March, sold23March_Discount),
      (is_more5, more5_Discount),
      (is_app, app_discount),
      (is_visa, visa_discount)
    )

    val discountRules = get_discountFunctions()

    // -------------------------- Processing orders with discounts --------------------------

    /**
     * Processes raw CSV lines into tuples of:
     * (product_name, total_before_discount, average_discount_percent, total_after_discount)
     * - Calculates total price before discount.
     * - Collects applicable discounts for each order.
     * - Averages top two discounts.
     * - Applies average discount to compute final price.
     */
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

    // -------------------------- Load processed data into database --------------------------

    /**
     * SQL insert statement for storing processed orders.
     */
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
