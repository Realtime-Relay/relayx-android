package com.relay.realtime.realtimeSDK

import io.nats.client.*
import io.nats.client.api.*
import com.google.gson.Gson
import io.nats.client.impl.NatsMessage
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.time.Instant

class Realtime(private val serverUrl: String) {

    private var connection: Connection? = null
    private var jetStream: JetStream? = null
    private val gson = Gson()

    private val reservedTopics = setOf(
        "CONNECTED", "RECONNECT", "MESSAGE_RESEND", "DISCONNECTED",
        "RECONNECTING", "RECONNECTED", "RECONN_FAIL"
    )

    private val listeners = ConcurrentHashMap<String, Dispatcher>()
    private val messageQueue = mutableListOf<Pair<String, Any>>() // Offline queue

    private val clientId: String = UUID.randomUUID().toString()


    init {
        connect()
    }

    private fun connect() {
        val opts = Options.Builder()
            .server(serverUrl)
            .connectionListener { _, eventType ->
                when (eventType) {
                    ConnectionListener.Events.CONNECTED -> resendQueuedMessages()
                    else -> {}
                }
            }
            .build()

        connection = Nats.connect(opts)
        jetStream = connection!!.jetStream(JetStreamOptions.defaultOptions())
    }

    fun publish(topic: String, message: Any?): Boolean {
        if (message == null) throw IllegalArgumentException("Message cannot be null")
        if (topic in reservedTopics) throw IllegalArgumentException("Cannot publish to SDK reserved topic: $topic")
        if (message !is String && message !is Number && message !is Map<*, *>)
            throw IllegalArgumentException("Messages must be of type string, number or JSON")

        val finalTopic = "${clientId.hashCode()}.$topic"
        val uuid = UUID.randomUUID().toString()
        val payload = mapOf(
            "client_id" to clientId,
            "id" to uuid,
            "room" to topic,
            "message" to message,
            "start" to Instant.now().epochSecond
        )

        return try {
            if (connection?.status == Connection.Status.CONNECTED) {
                jetStream?.publish(
                    NatsMessage.builder()
                    .subject(finalTopic)
                    .data(gson.toJson(payload))
                    .build()
                )
                true
            } else {
                messageQueue.add(Pair(topic, message))
                false
            }
        } catch (e: Exception) {
            false
        }
    }

    fun on(topic: String, listener: MessageListener) {
        if (topic.isBlank()) throw IllegalArgumentException("Invalid topic")

        val finalTopic = "${clientId.hashCode()}.$topic"
        val consumerConfig = ConsumerConfiguration.builder()
            .filterSubject(finalTopic)
            .deliverPolicy(DeliverPolicy.New)
            .ackPolicy(AckPolicy.Explicit)
            .build()

        val sub = jetStream!!.subscribe(finalTopic, PushSubscribeOptions.builder()
            .configuration(consumerConfig)
            .build()
        )

        val dispatcher = connection!!.createDispatcher { msg ->
            val data = gson.fromJson(String(msg.data), Map::class.java)
            val msgClientId = data["client_id"] as? String
            val room = data["room"] as? String

            if (msgClientId != clientId && room == topic) {
                listener.onMessage(data["message"])
                msg.ack()
            }
        }

        dispatcher.subscribe(finalTopic)
        listeners[topic] = dispatcher
    }

    fun off(topic: String): Boolean {
        if (topic.isBlank()) throw IllegalArgumentException("Invalid topic")
        val dispatcher = listeners.remove(topic) ?: return false
        try {
            dispatcher.unsubscribe(topic)
            return true
        } catch (e: Exception) {
            return false
        }
    }

    private fun resendQueuedMessages() {
        for ((topic, message) in messageQueue) {
            publish(topic, message)
        }
        messageQueue.clear()
    }

    interface MessageListener {
        fun onMessage(message: Any?)
    }
}
