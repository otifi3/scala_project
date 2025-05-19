package engine

import java.util.logging.{Logger, FileHandler, SimpleFormatter, Level}

/**
 * Object `AppLogger` configures and provides a centralized Logger instance.
 *
 * Logger details:
 * - Logger name: "OrderProcessingLogger"
 * - Logs written to file at "logs/rule_engine.log" (appending to existing file)
 * - Uses SimpleFormatter for log message formatting
 * - Parent handlers disabled to prevent duplicate logging to console
 * - Logging level set to INFO (logs INFO and above)
 *
 * Usage:
 *   Import and use `AppLogger.logger` to log messages throughout the application.
 *   Example: AppLogger.logger.info("Message")
 */
object AppLogger {
  val logger: Logger = {
    val log = Logger.getLogger("OrderProcessingLogger")

    val logPath = "logs/rule_engine.log"

    val fileHandler = new FileHandler(logPath, true)
    fileHandler.setFormatter(new SimpleFormatter())
    log.addHandler(fileHandler)
    log.setUseParentHandlers(false)
    log.setLevel(Level.INFO)
    log
  }
}
