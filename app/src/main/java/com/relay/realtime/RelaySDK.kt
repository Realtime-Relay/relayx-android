package com.relay.realtime

import com.relay.realtime.models.Message

object RelaySDK {

    private lateinit var connectionManager: RelayConnectionManager
    private lateinit var publisher: Publisher
    private lateinit var subscriber: Subscriber
    private lateinit var historyManager: HistoryManager

    fun initialize(jwt: String, secretKey: String) {
        val auth = RelayAuth(jwt, secretKey)
        connectionManager = RelayConnectionManager(auth)
        publisher = Publisher(connectionManager)
        subscriber = Subscriber(connectionManager)
        historyManager = HistoryManager(connectionManager)
    }

    fun connect() = connectionManager.connect()

    fun disconnect(manual: Boolean = true) = connectionManager.disconnect(manual)

    fun publish(topic: String, message: String) = publisher.publish(topic, message)

    fun subscribe(topic: String, listener: (Message) -> Unit): String =
        subscriber.subscribe(topic, listener)

    fun unsubscribe(subscriptionId: String) =
        subscriber.unsubscribe(subscriptionId)

    fun getHistory(topic: String, start: Long, end: Long? = null): List<Message> =
        historyManager.getHistory(topic, start, end)
}
