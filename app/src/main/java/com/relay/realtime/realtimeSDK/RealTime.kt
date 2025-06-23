package com.relay.realtime.realtimeSDK

import android.util.Log
import org.json.JSONObject
import java.util.*

class Realtime(
    private val apiKey: String,
    private val secretKey: String
) {

    init {
        require(apiKey.isNotBlank()) { "apiKey must be a non-empty string." }
        require(secretKey.isNotBlank()) { "secretKey must be a non-empty string." }
    }

    private var isDebug: Boolean = false
    private var isStaging: Boolean = false
    private var isConnected: Boolean = false // Mock connection state
    private val clientId: String = UUID.randomUUID().toString()
    private val activeListeners = mutableMapOf<String, Boolean>()

    fun init(staging: Boolean, opts: Map<String, Any>?) {
        requireNotNull(opts) { "Options (opts) are required." }
        isStaging = staging
        isDebug = opts["debug"] as? Boolean ?: false
        log("SDK initialized. Staging: $isStaging, Debug: $isDebug")
    }

    fun publish(topic: String, message: Any): Boolean {
        validateTopic(topic)
        validateMessage(message)

        if (isReservedTopic(topic)) {
            throw IllegalArgumentException("Cannot publish to reserved SDK topic: $topic")
        }

        val payload = JSONObject().apply {
            put("client_id", clientId)
            put("id", UUID.randomUUID().toString())
            put("room", topic)
            put("message", message)
            put("start", System.currentTimeMillis())
        }

        val finalTopic = getFinalTopic(topic)

        return if (isConnected) {
            startJetStreamIfNeeded()
            publishToJetStream(finalTopic, payload)
        } else {
            storeMessageLocally(finalTopic, payload)
            false
        }
    }

    fun on(topic: String, listener: MessageListener) {
        validateTopic(topic)
        require(listener != null) { "Listener cannot be null" }

        val finalTopic = getFinalTopic(topic)
        activeListeners[topic] = true

        if (isConnected) {
            log("Subscribing to JetStream topic: $finalTopic")

            // Mock message received
            val received = JSONObject().apply {
                put("client_id", "another-client")
                put("room", topic)
                put("message", "Sample relay message")
            }

            if (received.getString("client_id") != clientId && received.getString("room") == topic) {
                listener.onMessage(received.get("message"))
            }
        } else {
            log("Not connected, cannot setup listener for topic: $topic")
        }
    }

    fun off(topic: String): Boolean {
        validateTopic(topic)
        if (activeListeners[topic] != true) {
            log("No active listener to remove for topic: $topic")
            return false
        }

        val unsubscribed = unsubscribeConsumer(topic)
        return if (unsubscribed) {
            activeListeners[topic] = false
            log("Successfully unsubscribed from: $topic")
            true
        } else {
            log("Unsubscription failed for: $topic")
            false
        }
    }

    fun history(topic: String, startDate: Date?, endDate: Date?): List<JSONObject> {
        validateTopic(topic)
        requireNotNull(startDate) { "Start date cannot be null." }
        if (endDate != null && endDate.before(startDate)) {
            throw IllegalArgumentException("End date cannot be before start date.")
        }

        if (!isConnected) {
            log("Not connected. Returning empty history.")
            return emptyList()
        }

        val finalTopic = getFinalTopic(topic)
        val finalEnd = endDate ?: Date()

        return getJetStreamHistory(finalTopic, startDate, finalEnd)
    }

    interface MessageListener {
        fun onMessage(message: Any)
    }

    // -----------------------
    // Internal Methods
    // -----------------------

    private fun validateTopic(topic: String) {
        if (topic.isBlank() || topic.contains(" ") || topic.contains("*")) {
            throw IllegalArgumentException("Invalid topic name: must be non-empty and must not contain spaces or '*'.")
        }
    }

    private fun validateMessage(message: Any) {
        if (message !is String && message !is Number && message !is JSONObject) {
            throw IllegalArgumentException("Message must be a String, Number, or JSON object.")
        }
    }

    private fun isReservedTopic(topic: String): Boolean {
        val reserved = setOf(
            "CONNECTED", "RECONNECT", "MESSAGE_RESEND",
            "DISCONNECTED", "RECONNECTING", "RECONNECTED", "RECONN_FAIL"
        )
        return reserved.contains(topic)
    }

    private fun getFinalTopic(topic: String): String {
        val hash = (apiKey + secretKey).hashCode().toString()
        return "$hash.$topic"
    }

    private fun startJetStreamIfNeeded() {
        log("JetStream initialized (simulated).")
    }

    private fun publishToJetStream(finalTopic: String, payload: JSONObject): Boolean {
        log("Published to $finalTopic: $payload")
        return true
    }

    private fun storeMessageLocally(finalTopic: String, payload: JSONObject) {
        log("Stored message locally for $finalTopic")
    }

    private fun unsubscribeConsumer(topic: String): Boolean {
        log("Destroyed ephemeral consumer for topic: ${getFinalTopic(topic)}")
        return true
    }

    private fun getJetStreamHistory(topic: String, start: Date, end: Date): List<JSONObject> {
        val messages = mutableListOf<JSONObject>()
        try {
            val startMs = start.time
            val endMs = end.time

            for (i in 0..2) {
                val ts = startMs + (i * 1000)
                if (ts <= endMs) {
                    val msg = JSONObject().apply {
                        put("client_id", "history-client-$i")
                        put("id", UUID.randomUUID().toString())
                        put("room", topic)
                        put("message", "Historical message $i")
                        put("start", ts)
                    }
                    messages.add(msg)
                }
            }
        } catch (e: Exception) {
            log("History retrieval error: ${e.message}")
        }
        return messages
    }

    private fun log(message: String) {
        if (isDebug) {
            Log.d("RealtimeSDK", message)
        }
    }
}
