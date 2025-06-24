package com.relay.realtime.realtimeSDK

import android.util.Log
import kotlinx.coroutines.*
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
    private var isConnected: Boolean = false
    private val clientId: String = UUID.randomUUID().toString()
    private val activeListeners = mutableMapOf<String, Boolean>()
    private val localMessageStore = mutableListOf<Pair<String, JSONObject>>()
    private val messageListeners = mutableMapOf<String, MessageListener>()

    fun init(staging: Boolean, opts: Map<String, Any>?) {
        requireNotNull(opts) { "Options (opts) are required." }
        isStaging = staging
        isDebug = opts["debug"] as? Boolean ?: false
        log("SDK initialized. Staging: $isStaging, Debug: $isDebug")
    }

    /**
     * Connect asynchronously to Relay backend with given config.
     * Simulates a non-blocking connect with coroutine.
     */
    suspend fun connect() = withContext(Dispatchers.IO) {
        val baseUrls = if (isStaging) {
            (4221..4223).map { "0.0.0.0:$it" }
        } else {
            (4221..4223).map { "api.relay-x.io:$it" }
        }

        val connectionOptions = mapOf(
            "no_echo" to true,
            "max_reconnect_attempts" to 1200,
            "reconnect" to true,
            "reconnect_time_wait" to 1000,
            "token" to apiKey
        )

        log("Attempting to connect to Relay backend cluster: $baseUrls with options: $connectionOptions")

        delay(1000) // Simulate connection delay

        // Simulate successful connection:
        isConnected = true

        // Simulated namespace fetch (AND-FUNC-12)
        val namespace = getNamespace()
        log("Namespace obtained: $namespace")

        // Fire CONNECTED event
        fireSdkEvent("CONNECTED", "Connected to Relay backend")

        // Setup reconnect/disconnect listeners (simulated)
        setupConnectionEventListeners()

        // Subscribe to previously active topics
        activeListeners.filter { it.value }.keys.forEach { topic ->
            subscribeToTopic(topic)
        }

        log("Connection established successfully.")
    }

    private fun getNamespace(): String {
        // Mock: generate a namespace string, normally fetched from backend or config
        return "namespace-${apiKey.take(5)}"
    }

    private fun setupConnectionEventListeners() {
        // Simulated listeners
        // On disconnect:
        simulateDisconnectListener {
            isConnected = false
            fireSdkEvent("DISCONNECTED", "Disconnected from Relay backend")
            clearLocalMessages()
        }
        // On reconnecting:
        simulateReconnectListener {
            fireSdkEvent("RECONNECT", "RECONNECTING")
        }
        // On reconnect:
        simulateReconnectedListener {
            isConnected = true
            fireSdkEvent("RECONNECTED", "RECONNECTED")
            resendLocalMessages()
        }
    }

    private fun simulateDisconnectListener(onDisconnect: () -> Unit) {
        // This would be wired into NATS disconnect event
        // For demo, no actual trigger here
    }

    private fun simulateReconnectListener(onReconnectAttempt: () -> Unit) {
        // Wired into NATS reconnecting event
    }

    private fun simulateReconnectedListener(onReconnected: () -> Unit) {
        // Wired into NATS reconnect event
    }

    private fun fireSdkEvent(topic: String, message: Any) {
        messageListeners[topic]?.onMessage(message)
        log("SDK event fired: $topic with message: $message")
    }

    private fun clearLocalMessages() {
        localMessageStore.clear()
        log("Local stored messages cleared on disconnect.")
    }

    private fun resendLocalMessages() {
        log("Resending locally stored messages...")
        localMessageStore.forEach { (topic, payload) ->
            publishToJetStream(getFinalTopic(topic), payload)
        }
        localMessageStore.clear()
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

        activeListeners[topic] = true
        messageListeners[topic] = listener

        if (isConnected) {
            subscribeToTopic(topic)
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
        if (unsubscribed) {
            activeListeners[topic] = false
            messageListeners.remove(topic)
            log("Successfully unsubscribed from: $topic")
            return true
        } else {
            log("Unsubscription failed for: $topic")
            return false
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

    // ------------- Internal methods -------------

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
        localMessageStore.add(Pair(finalTopic, payload))
        log("Stored message locally for $finalTopic")
    }

    private fun unsubscribeConsumer(topic: String): Boolean {
        log("Destroyed ephemeral consumer for topic: ${getFinalTopic(topic)}")
        return true
    }

    private fun subscribeToTopic(topic: String) {
        log("Subscribed to topic: ${getFinalTopic(topic)}")
        // Actual subscription logic with JetStream would go here
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
