package ru.nsu.ccfit.pleshkov.net3

import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.launch
import ru.nsu.ccfit.pleshkov.net3.messages.*
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

private typealias _entry = MutableMap.MutableEntry<UUID, MessageToSend>?

internal class TreeNode(val name: String, val losses: Int, private val port: Int) {
    private val socket: DatagramSocket = DatagramSocket(port)
    private val messagesToSend = ArrayBlockingQueue<MessageToSend>(QUEUE_SIZE)
    private val messagesToAck: MutableMap<UUID, MessageToSend> = messagesToAckMap()
    private val receivedMessages = ArrayBlockingQueue<ReceivedMessage>(QUEUE_SIZE)
    private val knownGUIDS = ConcurrentHashMap<UUID, Unit>().keys
    private var parent: InetSocketAddress? = null
    private val children = ConcurrentHashMap<InetSocketAddress, String>()
    private val futureChildren = ArrayList<InetSocketAddress>()

    constructor(
            name: String,
            losses: Int,
            port: Int,
            parent: InetSocketAddress
    ) : this(name, losses, port) {
        this.parent = parent
    }

    fun start() {

    }

    private fun SendingRoutine() {
        while (!Thread.interrupted()) {
            val messageToSend = messagesToSend.take()
            val jsonMessage = messageToSend.messageHolder.toJson()
            val jsonBytes = jsonMessage.toByteArray()
            messagesToAck.put(messageToSend.guid, messageToSend)
            for (receiver in messageToSend.receivers) {
                socket.send(DatagramPacket(jsonBytes, jsonBytes.size, receiver))
            }
        }
    }

    private fun HandlingRoutine() {
        while(!Thread.interrupted()) {
            val message = receivedMessages.take()
            val sender = message.sender
            val dataMessage = message.message
            println("$dataMessage")
            val holder = MessageHolder(dataMessage, message.guid)
            val receivers = ArrayList<InetSocketAddress>(children.size)
            receivers.addAll(children.keys)
            val myParent = parent
            if(myParent != null) {
                receivers.add(myParent)
            }
            receivers.remove(sender)
            messagesToSend.add(MessageToSend(holder,receivers))
        }
    }

    private fun ReceivingRoutine() {
        while (!Thread.interrupted()) {
            val receivingPacket = DatagramPacket(ByteArray(BUFFER_SIZE), BUFFER_SIZE)
            socket.receive(receivingPacket)
            val jsonContent = String(receivingPacket.data)
            val sender = receivingPacket.socketAddress as? InetSocketAddress ?: continue
            val holder = jsonContent.fromJson(MessageHolder::class.java)
            val guid = holder.guid
            if(knownGUIDS.contains(guid)) {
                continue
            }
            val message = holder.parseMessage()
            if(message !is AckMessage) launch(CommonPool) {
                val ack = AckMessage(guid)
                val ackHolder = MessageHolder(ack)
                val json = ackHolder.toJson()
                val bytes = json.toByteArray()
                val packet = DatagramPacket(bytes, bytes.size, sender)
                socket.send(packet)
            }
            when(message) {
                is JoinMessage -> {
                    children.put(sender, message.name)
                }
                is AckMessage -> block@ {
                    val messageToSend = messagesToAck[guid] ?: return@block
                    val receivers = messageToSend.receivers
                    receivers.remove(sender)
                    if(receivers.isEmpty()) {
                        messagesToAck.remove(guid)
                    }
                }
                is DataMessage -> {
                    receivedMessages.add(ReceivedMessage(message, holder.guid, sender))
                }
                is AdoptMessage -> {
                    futureChildren.addAll(message.children)
                }
                is NewParentMessage -> {
                    parent = message.parent
                    val joinHolder = MessageHolder(JoinMessage(name))
                    val bytes = joinHolder.toJson().toByteArray()
                    socket.send(DatagramPacket(bytes, bytes.size, sender))
                }
            }
        }
    }

    private fun messagesToAckMap(): MutableMap<UUID, MessageToSend> = Collections.synchronizedMap(
            object : LinkedHashMap<UUID, MessageToSend>(QUEUE_SIZE) {
                override fun removeEldestEntry(p0: _entry) = size > QUEUE_SIZE
            })
}
