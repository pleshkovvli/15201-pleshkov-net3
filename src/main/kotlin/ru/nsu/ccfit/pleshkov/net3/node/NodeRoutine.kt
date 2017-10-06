package ru.nsu.ccfit.pleshkov.net3.node

import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal abstract class NodeRoutine(val node: TreeNode) : Runnable {
    protected val logger: Logger = LoggerFactory.getLogger(LOGGER_NAME)

    override fun run() {
        try {
            while (!Thread.interrupted()) {
                routine()
            }
        } catch (e: InterruptedException) {
            logger.debug("Thread ${Thread.currentThread().name} was interrupted")
        } catch (e: Exception) {
            logger.error("Something went wrong ${e.message}", e)
        }
    }

    protected abstract fun routine()
}