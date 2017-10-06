package ru.nsu.ccfit.pleshkov.net3.node

import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import ru.nsu.ccfit.pleshkov.net3.messages.DataMessage
import ru.nsu.ccfit.pleshkov.net3.messages.MessageHolder
import ru.nsu.ccfit.pleshkov.net3.messages.MessageToSend
import java.io.EOFException

private const val EXIT_STRING = "\$exit"
private const val DELAY_FOR_ADOPT_MS = 30000L

internal class InputRoutine(node: TreeNode) : NodeRoutine(node) {
    private val relatives = node.relatives
    private val messagesToSend = node.messagesToSend
    private val name = node.name

    override fun routine() {
        val string = readLine() ?: throw EOFException()
        if (string == EXIT_STRING) {
            if (!relatives.isAdopting()) {
                logger.info("EXIT NOW")
                node.exit()
            } else {
                logger.warn("EXIT LATER")
                launch(CommonPool) {
                    delay(DELAY_FOR_ADOPT_MS)
                    node.exit()
                }
            }
            throw InterruptedException()
        }
        val data = DataMessage(name, string)
        val receivers = relatives.childrenToList()
        relatives.toParent { receivers.add(it) }
        messagesToSend.add(MessageToSend(MessageHolder(data), receivers))
    }
}