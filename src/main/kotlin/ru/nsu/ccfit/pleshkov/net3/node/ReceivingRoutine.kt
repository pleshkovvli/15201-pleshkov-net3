package ru.nsu.ccfit.pleshkov.net3.node

import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import ru.nsu.ccfit.pleshkov.net3.LimitedLinkedHashMap
import ru.nsu.ccfit.pleshkov.net3.fromJson
import ru.nsu.ccfit.pleshkov.net3.messages.*
import ru.nsu.ccfit.pleshkov.net3.short
import ru.nsu.ccfit.pleshkov.net3.toJson
import java.net.DatagramPacket
import java.net.InetSocketAddress
import java.net.SocketTimeoutException
import java.util.*

internal class ReceivingRoutine(node: TreeNode) : NodeRoutine(node) {
    private val socket = node.socket
    private val losses = node.losses
    private val relatives = node.relatives
    private val acknowledgesController = node.ackController
    private val receivedMessages = node.receivedMessages
    private val chanceToLose = Random()
    private val knownGUIDS = LimitedLinkedHashMap<UUID, Unit>(QUEUE_SIZE)

    override fun routine() {
        val receivingPacket = DatagramPacket(ByteArray(BUFFER_SIZE), BUFFER_SIZE)

        try {
            socket.receive(receivingPacket)
        } catch (e: SocketTimeoutException) {
            return
        }

        if (chanceToLose.nextInt(100) < losses) {
            logger.warn("Dropping packet")
            return
        } else {
            logger.info("Got packet")
        }

        val (guid, message) = packetToMessageWithGuid(receivingPacket)
        val sender = receivingPacket.socketAddress as InetSocketAddress

        if (!(relatives.isRelative(sender) || relatives.isPassed(sender))
                && (message !is JoinMessage)) {
            logger.warn("Unknown sender")
            return
        }

        val notAck = message !is AckMessage
        if (notAck) {
            if(node.isExit) {
                logger.info("Dropping not ack message ${guid.short()} while exiting")
                return
            }
            launch(CommonPool) {
                val packet = messageToDatagram(AckMessage(guid), sender)
                logger.info("Sending ack on ${guid.short()}")
                socket.send(packet)
            }
        }

        if(relatives.isPassed(sender)) {
            logger.warn("Message from passed relative $sender")
            return
        }

        if (notAck && knownGUIDS.contains(guid)) {
            logger.info("Known guid ${guid.short()}: dropping message")
        } else {
            logger.info("New guid ${guid.short()}: processing message")
            knownGUIDS.put(guid, Unit)
            when (message) {
                is JoinMessage -> handleJoin(message, sender)
                is AckMessage -> handleAck(message, sender)
                is DataMessage -> handleData(message, guid, sender)
                is AdoptMessage -> handleAdopt(message, sender)
                is NewParentMessage -> handleNewParent(message, sender)
            }
        }
    }

    private fun handleNewParent(message: NewParentMessage, sender: InetSocketAddress) {
        logger.debug("NEW PARENT")

        if (!relatives.isParent(sender)) {
            logger.warn("Child tried to change parent")
            return
        }

        val newParent = message.parent
        relatives.parent = newParent
        launch(CommonPool) {
            node.join(newParent)
        }
    }

    private fun handleAdopt(message: AdoptMessage, sender: InetSocketAddress) {
        logger.debug("ADOPT")

        when {
           relatives.isParent(sender) -> {
               relatives.parent = null
               logger.info("NOW ROOT")
           }
           relatives.isChild(sender) -> {
               relatives.removeChild(sender)
           }
           else -> {
               return
           }
        }

        relatives.addFutureChildren(message.children)
    }

    private fun handleData(message: DataMessage, guid: UUID, sender: InetSocketAddress) {
        logger.debug("DATA")
        receivedMessages.add(ReceivedDataMessage(message, guid, sender))
    }

    private fun handleAck(message: AckMessage, sender: InetSocketAddress) {
        logger.debug("ACK")
        runBlocking {
            acknowledgesController.checkAck(message, sender)
        }
    }

    private fun handleJoin(message: JoinMessage, sender: InetSocketAddress) {
        logger.debug("JOIN")

        if (!relatives.isRelative(sender)) {
            relatives.addChild(sender, message.name)
            logger.info("Got child $sender")
        } else {
            if(relatives.isChild(sender)) {
                logger.info("Known child $sender")
            } else {
                logger.warn("Parent $sender tried to join")
            }
        }
    }

    private fun messageToDatagram(message: Message, sender: InetSocketAddress): DatagramPacket {
        val ackHolder = MessageHolder(message)
        val json = ackHolder.toJson()
        val bytes = json.toByteArray()
        return DatagramPacket(bytes, bytes.size, sender)
    }

    private fun packetToMessageWithGuid(packet: DatagramPacket): MessageWithGuid {
        val bytes = packet.data
        val jsonContent = String(bytes)
        val holder = jsonContent.fromJson(MessageHolder::class.java)
        return MessageWithGuid(holder.guid, holder.parseMessage())
    }
}