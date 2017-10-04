package ru.nsu.ccfit.pleshkov.net3

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock
import org.slf4j.LoggerFactory
import ru.nsu.ccfit.pleshkov.net3.messages.*
import java.io.EOFException
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.ArrayList
import kotlin.collections.LinkedHashMap

private const val BUFFER_SIZE: Int = 8 * 1024
private const val QUEUE_SIZE: Int = 100
private val logger = LoggerFactory.getLogger("Node")

internal class RelativesHolder(
        children: HashMap<InetSocketAddress, String>? = null,
        parent: InetSocketAddress? = null
) {
    private val children = HashMap<InetSocketAddress, String>()
    private var _parent: InetSocketAddress? = parent
    var parent: InetSocketAddress?
        get() = synchronized(this) {
            _parent
        }
        set(value) = synchronized(this) {
            _parent = value
        }

    init {
        if (children != null) {
            this.children.putAll(children)
        }
    }

    fun hasRelatives() : Boolean {
        return (!children.isEmpty()) || (_parent != null)
    }

    fun isChild(address: InetSocketAddress) = synchronized(this) {
        children.containsKey(address)
    }

    fun addChild(child: InetSocketAddress, name: String) = synchronized(this) {
        children.put(child, name)
    }

    fun isParent(address: InetSocketAddress) = synchronized(this) {
        _parent == address
    }

    fun isRelative(address: InetSocketAddress) = synchronized(this) {
        children.containsKey(address) || _parent == address
    }

    fun iterateChildren(block: (child: InetSocketAddress) -> Unit) = synchronized(this) {
        for (child in children.keys) {
            block(child)
        }
    }

    fun childrenToList(): ArrayList<InetSocketAddress> = synchronized(this) {
        val list = ArrayList<InetSocketAddress>()
        list.addAll(children.keys)
        return list
    }

    fun removeChild(child: InetSocketAddress) = synchronized(this) {
        children.remove(child)
    }

    fun toParent(block: (arg: InetSocketAddress) -> Unit) = synchronized(this) {
        if (_parent != null) {
            block(_parent!!)
        }
    }

    fun nextParent(): InetSocketAddress = synchronized(this) {
        if (_parent == null) {
            val nextParent = children.keys.first()
            children.remove(nextParent)
            nextParent
        } else {
            _parent!!
        }
    }
}

internal class TreeNode(
        val name: String,
        val losses: Int,
        port: Int,
        parent: InetSocketAddress? = null) {
    val socket = DatagramSocket(port)
    val messagesToSend = ArrayBlockingQueue<MessageToSend>(QUEUE_SIZE)
    val messagesToAck: MutableMap<UUID, MessageToSend>
            = Collections.synchronizedMap(LimitedLinkedHashMap<UUID, MessageToSend>(QUEUE_SIZE))
    val receivedMessages = ArrayBlockingQueue<ReceivedDataMessage>(QUEUE_SIZE)
    val relatives = RelativesHolder(parent = parent)
    val futureChildren = ConcurrentHashMap<InetSocketAddress, Unit>()
    val acksOnMessages = ConcurrentHashMap<UUID, ArrayList<InetSocketAddress>>()
    @Volatile
    var isExit: Boolean = false

    fun join(parent: InetSocketAddress?) {
        if (parent != null) {
            val joinHolder = MessageHolder(JoinMessage(name))
            val guid = joinHolder.guid
            acksOnMessages.put(guid, arrayListOf(parent))
            messagesToAck.put(guid, MessageToSend(joinHolder, arrayListOf(parent)))
            setAckCheck(guid)
            val bytes = joinHolder.toJson().toByteArray()
            socket.send(DatagramPacket(bytes, bytes.size, parent))
        }
    }

    fun start() {
        join(relatives.parent)
        Thread(InputRoutine(this), "Input").start()
        Thread(HandlingRoutine(this), "Handling").start()
        Thread(SendingRoutine(this), "Send").start()
        Thread(ReceivingRoutine(this), "Receive").start()
    }

    fun onExit() {
        if(!relatives.hasRelatives()) {
            logger.warn("EXIT: ISOLATED CHILD")
            System.exit(0) //TODO: Add proper exit
        }
        isExit = true
        val newParent: InetSocketAddress = relatives.nextParent()
        val newParentMessage = NewParentMessage(newParent)
        relatives.iterateChildren { child ->
            val holder = MessageHolder(newParentMessage)
            val bytes = holder.toJson().toByteArray()
            val guid = holder.guid
            messagesToAck.put(guid, MessageToSend(holder, arrayListOf(child)))
            setAckCheck(guid)
            socket.send(DatagramPacket(bytes, bytes.size, child))
        }
        val list = relatives.childrenToList()
        val adoptMessage = AdoptMessage(list)
        val adoptHolder = MessageHolder(adoptMessage)
        val adoptBytes = adoptHolder.toJson().toByteArray()
        val guid = adoptHolder.guid
        messagesToAck.put(guid, MessageToSend(adoptHolder, arrayListOf(newParent)))
        setAckCheck(guid)
        socket.send(DatagramPacket(adoptBytes, adoptBytes.size, newParent))
    }

    fun setAckCheck(guid: UUID) {
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
        when (messageToAck.code) {
            JOIN, ADOPT, NEW_PARENT -> handleMessage(guid, messageToAck) { a,b ->
                handleNonData(a,b)
            }
            DATA -> handleMessage(guid, messageToAck) {a,b ->
                handleData(a,b)
            }
        }
    }

    private fun handleMessage(
            guid: UUID,
            messageToAck: MessageToSend,
            block: (guid: UUID, messageToAck: MessageToSend) -> Unit
    ) {
        val acksOnMessage = acksOnMessages[guid]
        if (acksOnMessage != null && !acksOnMessage.isEmpty()) {
            messageToAck.receivers.clear()
            messageToAck.receivers.addAll(acksOnMessage)
            block(guid, messageToAck)
        } else {
            val status = if (messagesToAck.remove(guid, messageToAck)) {
                "success"
            } else "failure"
            logger.info("Removing $guid from messages to ack: $status")
            if (isExit && messagesToAck.isEmpty()) {
                logger.warn("EXIT")
                System.exit(0) //TODO: Add proper exit
            }
        }
    }

    private fun handleData(guid: UUID, messageToAck: MessageToSend) {
        val status = if (messagesToAck.remove(guid, messageToAck)) {
            "success"
        } else "failure"
        logger.info("Removing $guid from messages to ack to messagesToSend: $status")
        messagesToSend.add(messageToAck)
    }

    private fun handleNonData(guid: UUID, messageToAck: MessageToSend) {
        for (receiver in messageToAck.receivers) {
            val json = messageToAck.messageHolder.toJson()
            val bytes = json.toByteArray()
            logger.info("Resending $guid to $receiver")
            socket.send(DatagramPacket(bytes, bytes.size, receiver))
        }
        setAckCheck(guid)
    }
}

private val mutex = Mutex()

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

private class LimitedLinkedHashMap<K, V>(val capacity: Int) : LinkedHashMap<K, V>(capacity) {
    override fun removeEldestEntry(p0: MutableMap.MutableEntry<K, V>?) = size > capacity
}

private abstract class Routine : Runnable {
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

private abstract class NodeRoutine(val node: TreeNode) : Routine()

private class HandlingRoutine(node: TreeNode) : NodeRoutine(node) {
    val receivedMessages = node.receivedMessages
    val messagesToSend = node.messagesToSend
    val relatives = node.relatives

    override fun routine() {
        val message = receivedMessages.take()
        val sender = message.sender
        val dataMessage = message.message
        println("$dataMessage")
        val holder = MessageHolder(dataMessage, message.guid)
        val receivers = relatives.childrenToList()
        relatives.toParent { receivers.add(it) }
        receivers.remove(sender)
        messagesToSend.add(MessageToSend(holder, receivers))
    }
}

private class SendingRoutine(node: TreeNode) : NodeRoutine(node) {
    val messagesToSend = node.messagesToSend
    val messagesToAck = node.messagesToAck
    val acksOnMessages = node.acksOnMessages
    val socket = node.socket

    override fun routine() {
        val messageToSend = messagesToSend.take()
        val jsonMessage = messageToSend.messageHolder.toJson()
        val jsonBytes = jsonMessage.toByteArray()
        val guid = messageToSend.guid
        logger.info("Putting $guid in messagesToAck")
        messagesToAck.put(guid, messageToSend)
        if (!acksOnMessages.containsKey(guid)) {
            logger.info("PUTTING $guid in acksOnMessages with ${messageToSend.receivers}")
            acksOnMessages.put(guid, ArrayList(messageToSend.receivers))
        }
        node.setAckCheck(guid)
        for (receiver in messageToSend.receivers) {
            socket.send(DatagramPacket(jsonBytes, jsonBytes.size, receiver))
        }
    }
}

private class InputRoutine(node: TreeNode) : NodeRoutine(node) {
    val futureChildren = node.futureChildren
    val relatives = node.relatives
    val messagesToSend = node.messagesToSend

    override fun routine() {
        val string = readLine() ?: throw EOFException()
        if (string == "\$exit") {
            if (futureChildren.isEmpty()) {
                node.onExit()
            } else launch(CommonPool) {
                delay(30000)
                node.onExit()
            }
            return
        }
        val data = DataMessage(node.name, string)
        val receivers = relatives.childrenToList()
        relatives.toParent { receivers.add(it) }
        messagesToSend.add(MessageToSend(MessageHolder(data), receivers))
    }
}

private class ReceivingRoutine(node: TreeNode) : NodeRoutine(node) {
    val socket = node.socket
    val losses = node.losses
    val relatives = node.relatives
    val futureChildren = node.futureChildren
    val acksOnMessages = node.acksOnMessages
    val messagesToAck = node.messagesToAck
    val receivedMessages = node.receivedMessages
    val chanceToLose = Random()
    val knownGUIDS = LimitedLinkedHashMap<UUID, Unit>(QUEUE_SIZE)

    override fun routine() {
        val receivingPacket = DatagramPacket(ByteArray(BUFFER_SIZE), BUFFER_SIZE)
        socket.receive(receivingPacket)
        if (chanceToLose.nextInt(100) < losses) {
            logger.warn("Dropping packet")
            return
        } else {
            logger.info("Got packet")
        }
        val (guid, message) = packetToMessageWithGuid(receivingPacket)
        val sender = receivingPacket.socketAddress as InetSocketAddress
        if (!relatives.isRelative(sender) && (message !is JoinMessage)) {
            logger.warn("Unknown sender")
            return
        }
        val notAck = message !is AckMessage
        if (notAck && node.isExit) {
            return
        }
        if (notAck) launch(CommonPool) {
            val packet = messageToDatagram(AckMessage(guid), sender)
            logger.info("Sending ack on $guid")
            socket.send(packet)
        }
        if (notAck && knownGUIDS.contains(guid)) {
            logger.info("Known guid $guid: dropping message")
            return
        } else {
            logger.info("New guid $guid: processing message")
            knownGUIDS.put(guid, Unit)
        }
        when (message) {
            is JoinMessage -> handleJoin(message, sender)
            is AckMessage -> handleAck(message, sender)
            is DataMessage -> handleData(message, guid, sender)
            is AdoptMessage -> handleAdopt(message, sender)
            is NewParentMessage -> handleNewParent(message, sender)
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
           relatives.isParent(sender) -> relatives.parent = null
           relatives.isChild(sender) -> relatives.removeChild(sender)
           else -> return
        }
        for(child in message.children) {
            futureChildren.put(child, Unit)
        }
    }

    private fun handleData(message: DataMessage, guid: UUID, sender: InetSocketAddress) {
        logger.debug("DATA")
        receivedMessages.add(ReceivedDataMessage(message, guid, sender))
    }

    private fun handleAck(message: AckMessage, sender: InetSocketAddress) {
        logger.debug("ACK")
        runBlocking {
            mutex.withLock {
                val guidToAck = message.guidOfAck
                val list = acksOnMessages[guidToAck]
                if (list != null) {
                    logger.info("Removing $sender from message $guidToAck")
                    list.remove(sender)
                }
            }
        }
    }

    private fun handleJoin(message: JoinMessage, sender: InetSocketAddress) {
        logger.debug("JOIN")
        if (!relatives.isChild(sender)) {
            futureChildren.remove(sender)
            relatives.addChild(sender, message.name)
            logger.info("Got child $sender")
        } else {
            logger.info("Known child $sender")
        }
    }
}
