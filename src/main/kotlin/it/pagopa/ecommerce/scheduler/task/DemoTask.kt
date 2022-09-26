package it.pagopa.ecommerce.scheduler.task

import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.text.SimpleDateFormat
import java.util.*


@Component
class DemoTask {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val dateFormat = SimpleDateFormat("HH:mm:ss")

    //@Scheduled(fixedRate = 5000)
    private fun reportCurrentTime() {
        logger.info("Demo Scheduled task: execution time: {}", dateFormat.format(Date()))
    }
}