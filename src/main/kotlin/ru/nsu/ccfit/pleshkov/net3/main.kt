package ru.nsu.ccfit.pleshkov.net3

import java.net.InetSocketAddress

fun main(args: Array<String>) {
    if (args.size < 3) {
        println("Usage: java -jar net3.jar NAME LOSS% PORT [PARENT IP:PORT]")
        return
    }
    val losses = args[1].toIntOrNull()
    if (losses == null || losses !in 0..99) {
        println("LOSS% = ${args[1]} is not a number in range 0..99")
        return
    }
    try {
        val port = getPort(args[2])
        val parentIpAndPort: InetSocketAddress? = if (args.size == 3)
            null else getIpAndPort(args[3])
        TreeNode(args[0], losses, port, parentIpAndPort).start()
    } catch (e: InitializationException) {
        println(e.message)
    }
}
