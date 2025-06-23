package com.relay.realtime.realtimeSDK

import android.util.Log
import org.json.JSONObject

class Realtime(private val apiKey: String?, private val secretKey: String?) {

    private var isDebug: Boolean = false
    private var isInitialized: Boolean = false

    init {
        if (apiKey.isNullOrEmpty()) {
            throw IllegalArgumentException("API key must not be null or empty")
        }

        if (secretKey.isNullOrEmpty()) {
            throw IllegalArgumentException("Secret key must not be null or empty")
        }
    }

    /**
     * Initialize the SDK with environment and options.
     *
     * @param staging Boolean flag for staging environment.
     * @param opts JSON object with optional settings (e.g., debug).
     */
    fun init(staging: Boolean, opts: JSONObject?) {
        if (opts == null) {
            throw IllegalArgumentException("Options (opts) must not be null")
        }

        if (opts.has("debug")) {
            isDebug = opts.optBoolean("debug", false)
        }

        log("SDK initialized in ${if (staging) "STAGING" else "PRODUCTION"} mode")
        isInitialized = true
    }

    /**
     * Publish a message to a topic.
     */
    fun publish(topic: String?, message: Any?) {
        validateInit()
        validateTopic(topic)
        validateMessage(message)

        log("Publishing message to topic [$topic]: $message")
        // Actual message publishing logic goes here
    }

    // PRIVATE HELPERS

    private fun log(message: String) {
        if (isDebug) {
            Log.d("RealtimeSDK", message)
        }
    }

    private fun validateInit() {
        if (!isInitialized) {
            throw IllegalStateException("SDK not initialized. Call init() before using other methods.")
        }
    }

    private fun validateTopic(topic: String?) {
        if (topic.isNullOrEmpty()) {
            throw IllegalArgumentException("Topic must not be null or empty") // AND-FUNC-03
        }

        if (topic.contains(" ") || topic.contains("*")) {
            throw IllegalArgumentException("Topic must not contain spaces or '*' character") // AND-FUNC-03-1
        }
    }

    private fun validateMessage(message: Any?) {
        when (message) {
            is String, is Number, is JSONObject -> return
            else -> throw IllegalArgumentException("Message must be a String, Number, or JSON object") // AND-FUNC-04
        }
    }
}
