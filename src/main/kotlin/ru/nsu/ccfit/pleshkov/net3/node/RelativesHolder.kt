package ru.nsu.ccfit.pleshkov.net3.node

import ru.nsu.ccfit.pleshkov.net3.LimitedLinkedHashMap
import java.net.InetSocketAddress
import java.util.HashMap

private const val PASSED_RELATIVES_NUMBER = 20

internal class RelativesHolder(parent: InetSocketAddress? = null) {
    private val lock = Any()

    private val passedRelatives
            = LimitedLinkedHashMap<InetSocketAddress, Unit>(PASSED_RELATIVES_NUMBER)
    private val futureChildren = HashMap<InetSocketAddress, Unit>()
    private val children = HashMap<InetSocketAddress, String>()

    private var _parent: InetSocketAddress? = parent
    var parent: InetSocketAddress?
        get() = synchronized(lock) {
            _parent
        }
        set(value) = synchronized(lock) {
            if(_parent != null) {
                passedRelatives.put(_parent!!, Unit)
            }
            _parent = value
        }

    fun isAdopting() = synchronized(lock) { futureChildren.isNotEmpty() }

    fun hasRelatives() = synchronized(lock) { !children.isEmpty() || (_parent != null) }

    fun isPassed(address: InetSocketAddress) = synchronized(lock) {
        passedRelatives.containsKey(address)
    }

    fun isRelative(address: InetSocketAddress) = synchronized(lock) {
        children.containsKey(address) || _parent == address
    }

    fun isParent(address: InetSocketAddress) = synchronized(lock) { _parent == address }

    fun isChild(address: InetSocketAddress) = synchronized(lock) {
        children.containsKey(address)
    }

    fun addFutureChildren(newChildren: ArrayList<InetSocketAddress>) = synchronized(lock) {
        for (child in newChildren) {
            if(!children.containsKey(child)) {
                futureChildren.put(child, Unit)
            }
        }
    }

    fun addChild(child: InetSocketAddress, name: String) = synchronized(lock) {
        futureChildren.remove(child)
        children.put(child, name)
    }

    fun childrenToList(): ArrayList<InetSocketAddress> = synchronized(lock) {
        val list = ArrayList<InetSocketAddress>()
        list.addAll(children.keys)
        return list
    }

    fun removeChild(child: InetSocketAddress) = synchronized(lock) {
        passedRelatives.put(child, Unit)
        children.remove(child)
    }

    fun toParent(block: (arg: InetSocketAddress) -> Unit) = synchronized(lock) {
        if (_parent != null) {
            block(_parent!!)
        }
    }

    fun nextParent(): InetSocketAddress = synchronized(lock) {
        if (_parent == null) {
            val nextParent = children.keys.first()
            children.remove(nextParent)
            _parent = nextParent
            nextParent
        } else {
            _parent!!
        }
    }
}