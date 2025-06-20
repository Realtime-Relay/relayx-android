package com.relay.realtime

import org.json.JSONObject

class Publisher(private val connectionManager: RelayConnectionManager) {
    fun publish(topic: String, message: String) {
        val payload = JSONObject()
            .put("type", "publish")
            .put("topic", topic)
            .put("message", message)
        connectionManager.send(payload.toString())
    }
}
