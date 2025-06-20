package com.relay.realtime

import com.relay.realtime.models.Message
import org.json.JSONObject

class HistoryManager(private val connectionManager: RelayConnectionManager) {

    fun getHistory(topic: String, start: Long, end: Long? = null): List<Message> {
        val payload = JSONObject()
            .put("type", "history")
            .put("topic", topic)
            .put("start", start)
        end?.let { payload.put("end", it) }

        // You might need a mechanism to block/wait and parse response if backend is async.
        connectionManager.send(payload.toString())

        return emptyList() // Placeholder for actual parsed response
    }
}
