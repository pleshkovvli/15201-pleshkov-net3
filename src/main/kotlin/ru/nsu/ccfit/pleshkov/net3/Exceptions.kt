package ru.nsu.ccfit.pleshkov.net3

open class InitializationException(message: String) : Exception(message)
class InvalidPortException(message: String) : InitializationException(message)
class UnknownCodeException(code: Byte) : Exception("Unknown code = $code")