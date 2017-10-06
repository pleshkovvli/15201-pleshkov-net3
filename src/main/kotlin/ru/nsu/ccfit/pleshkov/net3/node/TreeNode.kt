package ru.nsu.ccfit.pleshkov.net3.node

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.nsu.ccfit.pleshkov.net3.messages.*
import ru.nsu.ccfit.pleshkov.net3.toJson
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.util.concurrent.ArrayBlockingQueue

const val BUFFER_SIZE: Int = 8 * 1024
const val QUEUE_SIZE: Int = 100

internal const val LOGGER_NAME = "node"

internal class TreeNode(
        val name: String,
        val losses: Int,
        port: Int,
        parent: InetSocketAddress? = null) {

    val socket = DatagramSocket(port)
    val ackController = AcknowledgesController(this)
    val relatives = RelativesHolder(parent)
    val messagesToSend = ArrayBlockingQueue<MessageToSend>(QUEUE_SIZE)
    val receivedMessages = ArrayBlockingQueue<ReceivedDataMessage>(QUEUE_SIZE)
    private val threads = ArrayList<Thread>()

    private val logger: Logger = LoggerFactory.getLogger(LOGGER_NAME)

    @Volatile
    var isExit: Boolean = false

    fun join(parent: InetSocketAddress) {
        val joinHolder = MessageHolder(JoinMessage(name))
        ackController.setAckCheck(MessageToSend(joinHolder, arrayListOf(parent)))
        val bytes = joinHolder.toJson().toByteArray()
        socket.send(DatagramPacket(bytes, bytes.size, parent))
    }

    fun start() {
        socket.soTimeout = 1000
        val parent = relatives.parent
        if(parent == null) {
            logger.info("ROOT")
        } else {
            logger.info("CHILD of $parent")
            join(parent)
        }

        threads.add(Thread(InputRoutine(this), "Input"))
        threads.add(Thread(HandlingRoutine(this), "Handling"))
        threads.add(Thread(SendingRoutine(this), "Send"))
        threads.add(Thread(ReceivingRoutine(this), "Receive"))
        for(thread in threads) {
            thread.start()
        }
    }

    fun finishThreads() {
        val currentThread = Thread.currentThread()
        for(thread in threads) {
            if(currentThread.id != thread.id) {
                thread.interrupt()
            }
        }
    }

    fun exit() {
        if(!relatives.hasRelatives()) {
            logger.warn("EXIT: ROOT")
            finishThreads()
            return
        }

        isExit = true
        val newParent: InetSocketAddress = relatives.nextParent()
        val children = relatives.childrenToList()

        val newParentMessage = NewParentMessage(newParent)
        val newParentHolder = MessageHolder(newParentMessage)
        val newParentBytes = newParentHolder.toJson().toByteArray()

        ackController.setAckCheck(MessageToSend(newParentHolder, ArrayList(children)))
        for(child in children) {
            socket.send(DatagramPacket(newParentBytes, newParentBytes.size, child))
        }

        val adoptMessage = AdoptMessage(ArrayList(children))
        val adoptHolder = MessageHolder(adoptMessage)
        val adoptBytes = adoptHolder.toJson().toByteArray()

        ackController.setAckCheck(MessageToSend(adoptHolder, arrayListOf(newParent)))
        socket.send(DatagramPacket(adoptBytes, adoptBytes.size, newParent))
    }
}
