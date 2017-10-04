package ru.nsu.ccfit.pleshkov.net3.messages

import com.fasterxml.jackson.annotation.JsonCreator
import ru.nsu.ccfit.pleshkov.net3.UnknownCodeException
import ru.nsu.ccfit.pleshkov.net3.fromJson
import ru.nsu.ccfit.pleshkov.net3.toJson
import java.net.InetSocketAddress
import java.util.*
import kotlin.collections.ArrayList

private const val ERROR: Byte = -1
internal const val JOIN: Byte = 0
internal const val ACK: Byte = 1
internal const val DATA: Byte = 2
internal const val ADOPT: Byte = 3
internal const val NEW_PARENT: Byte = 4

internal class MessageHolder {
    val code: Byte
    val guid: UUID
    val jsonMessage: String
    constructor(message: Message, guid: UUID  = UUID.randomUUID()) {
        this.guid = guid
        code = message.code
        jsonMessage = message.toJson()
    }
    @JsonCreator
    constructor(code: Byte, guid: UUID, jsonMessage: String) {
        this.code = code
        this.guid = guid
        this.jsonMessage = jsonMessage
    }
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

internal class ReceivedDataMessage(val message: DataMessage, val guid: UUID, val sender: InetSocketAddress)
internal data class MessageToSend(val messageHolder: MessageHolder, val receivers: ArrayList<InetSocketAddress>) {
    val guid get() = messageHolder.guid
    val code get() = messageHolder.code
}

internal data class MessageWithGuid(val guid: UUID, val message: Message)
internal sealed class Message(val code: Byte)
internal data class JoinMessage(val name: String) : Message(JOIN)
internal data class AckMessage(val guidOfAck: UUID) : Message(ACK)
internal data class DataMessage(val sender: String, val message: String) : Message(DATA) {
    override fun toString(): String {
        return "\nFROM $sender\n$message\n"
    }
}
internal data class AdoptMessage(val children: ArrayList<InetSocketAddress>) : Message(ADOPT)
internal data class NewParentMessage(val parent: InetSocketAddress?) : Message(NEW_PARENT)
