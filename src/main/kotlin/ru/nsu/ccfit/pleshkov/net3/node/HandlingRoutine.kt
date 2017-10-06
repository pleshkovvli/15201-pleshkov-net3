package ru.nsu.ccfit.pleshkov.net3.node

import ru.nsu.ccfit.pleshkov.net3.messages.MessageHolder
import ru.nsu.ccfit.pleshkov.net3.messages.MessageToSend

internal class HandlingRoutine(node: TreeNode) : NodeRoutine(node) {
    private val receivedMessages = node.receivedMessages
    private val messagesToSend = node.messagesToSend
    private val relatives = node.relatives

    override fun routine() {
        val message = receivedMessages.take()
        val sender = message.sender
        val dataMessage = message.message
        dataMessage.printItColorful()
        val holder = MessageHolder(dataMessage, message.guid)
        val receivers = relatives.childrenToList()
        relatives.toParent { receivers.add(it) }
        receivers.remove(sender)
        messagesToSend.add(MessageToSend(holder, receivers))
    }
}