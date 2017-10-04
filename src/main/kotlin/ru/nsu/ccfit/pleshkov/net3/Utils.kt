package ru.nsu.ccfit.pleshkov.net3

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.UnknownHostException


private object JacksonAPI {
    private val mapper = jacksonObjectMapper()
    private val writer = mapper.writerWithDefaultPrettyPrinter()
    fun toJson(value: Any) : String = writer.writeValueAsString(value)
    fun <T> fromJson(json: String, clazz: Class<T>) : T = mapper.readValue<T>(json, clazz)
}

fun Any.toJson() = JacksonAPI.toJson(this)
fun <T> String.fromJson(clazz: Class<T>) = JacksonAPI.fromJson(this, clazz)

fun getPort(portString: String) : Int {
    val port = portString.toIntOrNull()
    if(port == null || port !in 0..65535) {
        throw InvalidPortException("$portString is not a port number")
    }
    return port
}

fun getIpAndPort(ipAndPort: String) : InetSocketAddress {
    val ipString = ipAndPort.substringBefore(':', "")
    val portString = ipAndPort.substringAfter(':', "")
    if(ipString.isEmpty() || portString.isEmpty()) {
        throw InitializationException("Address should be in format: IP:PORT")
    }
    val ip = try {
        InetAddress.getByName(ipString)
    } catch (e: UnknownHostException) {
        throw InitializationException("Couldn't find host at $ipString")
    }
    val port = getPort(portString)
    return InetSocketAddress(ip, port)
}
