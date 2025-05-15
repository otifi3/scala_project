package engine

import java.util.logging.{Logger, FileHandler, SimpleFormatter, Level}

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
