package ru.nsu.ccfit.pleshkov.net3.messages

import ru.nsu.ccfit.pleshkov.net3.UnknownCodeException
import ru.nsu.ccfit.pleshkov.net3.fromJson
import ru.nsu.ccfit.pleshkov.net3.toJson
import java.net.InetSocketAddress
import java.util.*
import kotlin.collections.ArrayList

private const val JOIN: Byte = 0
private const val ACK: Byte = 1
private const val DATA: Byte = 2
private const val ADOPT: Byte = 3
private const val NEW_PARENT: Byte = 4

internal class MessageHolder(message: Message, val guid: UUID = UUID.randomUUID()) {
    private val code: Byte = message.code
    private val jsonMessage: String = message.toJson()
    fun parseMessage(): Message {
        val clazz = when(code) {
            JOIN -> JoinMessage::class.java
            ACK -> AckMessage::class.java
            DATA -> DataMessage::class.java
            ADOPT -> AdoptMessage::class.java
            NEW_PARENT -> NewParentMessage::class.java
            else -> throw UnknownCodeException(code)
        }
        return jsonMessage.fromJson(clazz)
    }
}

internal class ReceivedMessage(val message: DataMessage, val guid: UUID, val sender: InetSocketAddress)

internal class MessageToSend(
        val messageHolder: MessageHolder,
        val receivers: ArrayList<InetSocketAddress>
) {
    val guid get() = messageHolder.guid
}

internal sealed class Message(val code: Byte)
internal data class JoinMessage(val name: String) : Message(JOIN)
internal data class AckMessage(val guidOfAck: UUID) : Message(ACK)
internal data class DataMessage(val message: String) : Message(DATA)
internal data class AdoptMessage(val children: ArrayList<InetSocketAddress>) : Message(ADOPT)
internal data class NewParentMessage(val parent: InetSocketAddress) : Message(NEW_PARENT)
