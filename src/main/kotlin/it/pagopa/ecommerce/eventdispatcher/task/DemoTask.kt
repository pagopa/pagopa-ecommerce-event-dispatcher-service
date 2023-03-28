package it.pagopa.ecommerce.eventdispatcher.task

import java.text.SimpleDateFormat
import java.util.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class DemoTask {
  private val logger = LoggerFactory.getLogger(javaClass)
  private val dateFormat = SimpleDateFormat("HH:mm:ss")

  // @Scheduled(fixedRate = 5000)
  private fun reportCurrentTime() {
    logger.info("Demo Scheduled task: execution time: {}", dateFormat.format(Date()))
  }
}
