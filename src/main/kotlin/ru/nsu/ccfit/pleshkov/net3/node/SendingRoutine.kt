package ru.nsu.ccfit.pleshkov.net3.node

import ru.nsu.ccfit.pleshkov.net3.short
import ru.nsu.ccfit.pleshkov.net3.toJson
import java.net.DatagramPacket

internal class SendingRoutine(node: TreeNode) : NodeRoutine(node) {
    private val messagesToSend = node.messagesToSend
    private val socket = node.socket

    override fun routine() {
        val messageToSend = messagesToSend.take()
        val jsonMessage = messageToSend.messageHolder.toJson()
        val jsonBytes = jsonMessage.toByteArray()
        val guid = messageToSend.guid
        logger.info("Putting ${guid.short()} in messagesToAck")
        node.ackController.setAckCheck(messageToSend)
        for (receiver in messageToSend.receivers) {
            socket.send(DatagramPacket(jsonBytes, jsonBytes.size, receiver))
        }
    }
}