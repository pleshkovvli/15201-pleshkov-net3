package ru.nsu.ccfit.pleshkov.net3.node

import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.nsu.ccfit.pleshkov.net3.LimitedLinkedHashMap
import ru.nsu.ccfit.pleshkov.net3.messages.*
import ru.nsu.ccfit.pleshkov.net3.short
import ru.nsu.ccfit.pleshkov.net3.toJson
import java.net.DatagramPacket
import java.net.InetSocketAddress
import java.util.*
import kotlin.collections.ArrayList

internal class AcknowledgesController(private val node: TreeNode) {
    private val logger: Logger = LoggerFactory.getLogger(LOGGER_NAME)
    private val mutex = Mutex()
    private val acksOnMessages = HashMap<UUID, ArrayList<InetSocketAddress>>()
    private val messagesToAck= LimitedLinkedHashMap<UUID, MessageToSend>(QUEUE_SIZE)

    fun checkAck(message: AckMessage, sender: InetSocketAddress) = runBlocking {
        processAck(message, sender)
    }

    fun setAckCheck(messageToSend: MessageToSend) = runBlocking { setAck(messageToSend) }

    private suspend fun processAck(message: AckMessage, sender: InetSocketAddress) = mutex.withLock {
        val guidOfAck = message.guidOfAck
        val list = acksOnMessages[guidOfAck]

        if (list != null) {
            if(list.remove(sender)) {
                logger.info("Removing $sender from message ${guidOfAck.short()}")
            } else {
                logger.warn("Couldn't find $sender in message ${guidOfAck.short()}")
            }
        }
    }

    private suspend fun setAck(messageToSend: MessageToSend) = mutex.withLock {
        val guid = messageToSend.guid

        if (!acksOnMessages.containsKey(guid)) {
            logger.info("PUTTING ${guid.short()} in acksOnMessages with ${messageToSend.receivers}")
            acksOnMessages.put(guid, ArrayList(messageToSend.receivers))
        }
        messagesToAck.put(guid, messageToSend)
        launchAckCheck(guid)
    }

    private fun launchAckCheck(guid: UUID) {
        launch(CommonPool) {
            delay(500)
            mutex.withLock {
                try {
                    checkMessage(guid)
                } catch (e: Exception) {
                    logger.warn("Failed to check ack", e)
                }
            }
        }
    }

    private fun checkMessage(guid: UUID) {
        val messageToAck = messagesToAck[guid] ?: return
        handleMessage(messageToAck)
    }

    private fun Boolean.asSuccess() = if(this) "success" else "failure"

    private fun handleMessage(messageToAck: MessageToSend) {
        val guid = messageToAck.guid
        val acksOnMessage = acksOnMessages[guid]

        if (acksOnMessage != null && !acksOnMessage.isEmpty()) {
            messageToAck.receivers.clear()
            messageToAck.receivers.addAll(acksOnMessage)
            when(messageToAck.code) {
                JOIN, ADOPT, NEW_PARENT -> handleNonData(guid, messageToAck)
                DATA -> handleData(guid, messageToAck)
            }
        } else {
            val status = messagesToAck.remove(guid, messageToAck).asSuccess()
            logger.info("Removing ${guid.short()} from messages to ack: $status")
            if (node.isExit && messagesToAck.isEmpty()) {
                logger.warn("EXIT")
                node.finishThreads()
            }
        }
    }

    private fun handleData(guid: UUID, messageToAck: MessageToSend) {
        val status = messagesToAck.remove(guid, messageToAck).asSuccess()
        logger.info("Removing ${guid.short()} from messages to ack to messagesToSend: $status")
        node.messagesToSend.add(messageToAck)
    }

    private fun handleNonData(guid: UUID, messageToAck: MessageToSend) {
        for (receiver in messageToAck.receivers) {
            val json = messageToAck.messageHolder.toJson()
            val bytes = json.toByteArray()
            logger.info("Resending ${guid.short()} to $receiver")
            node.socket.send(DatagramPacket(bytes, bytes.size, receiver))
        }
        launchAckCheck(guid)
    }
}