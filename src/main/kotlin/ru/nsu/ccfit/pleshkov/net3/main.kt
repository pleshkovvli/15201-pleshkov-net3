package ru.nsu.ccfit.pleshkov.net3

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import ru.nsu.ccfit.pleshkov.net3.messages.*
import java.net.InetSocketAddress
import java.util.*


fun main(args: Array<String>) {
    val m: Message = if(args.size < 2) JoinMessage("sd") else AckMessage(UUID.randomUUID())
    val d = when(m) {
        is JoinMessage -> ""
        is AckMessage -> "sd"
        is DataMessage -> "fds"
        is AdoptMessage -> "sdfsdf"
        is NewParentMessage -> "df"
    }

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


    } catch (e: InitializationException) {
        println(e.message)
    }
}
