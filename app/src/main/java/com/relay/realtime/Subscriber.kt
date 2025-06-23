package com.relay.realtime

import com.relay.realtime.models.Message
import org.json.JSONObject
import java.util.UUID

class Subscriber(private val connectionManager: RelayConnectionManager) {

    private val listeners = mutableMapOf<String, (Message) -> Unit>()
    private val subscriptions = mutableMapOf<String, String>() // id -> topic

    fun subscribe(topic: String, listener: (Message) -> Unit): String {
        val subId = UUID.randomUUID().toString()
        listeners[subId] = listener
        subscriptions[subId] = topic

        val payload = JSONObject()
            .put("type", "subscribe")
            .put("topic", topic)
            .put("subscriptionId", subId)

        connectionManager.send(payload.toString())
        return subId
    }

    fun unsubscribe(subscriptionId: String) {
        val topic = subscriptions[subscriptionId] ?: return
        val payload = JSONObject()
            .put("type", "unsubscribe")
            .put("topic", topic)
            .put("subscriptionId", subscriptionId)

        connectionManager.send(payload.toString())

        listeners.remove(subscriptionId)
        subscriptions.remove(subscriptionId)
    }

    fun onMessageReceived(json: JSONObject) {
        val subId = json.optString("subscriptionId")
        val topic = json.optString("topic")
        val message = json.optString("message")

        listeners[subId]?.invoke(Message(topic, message))
    }
}
