package com.relay.realtime

import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Options
import java.nio.charset.StandardCharsets

class Realtime(private val apiKey: String, private val secretKey: String) {

    private var connection: Connection? = null

    init {
        require(apiKey.isNotBlank()) { "apiKey must not be empty" }
        require(secretKey.isNotBlank()) { "secretKey must not be empty" }
    }

    fun connect(natsUrl: String = "nats://api.relay-x.io:4222") {
        val opts = Options.Builder()
            .server(natsUrl)
            .userInfo(apiKey.toCharArray(), secretKey.toCharArray()) // Or use .token()
            .build()

        connection = Nats.connect(opts)
    }

    fun publish(subject: String, message: String) {
        val conn = connection ?: throw IllegalStateException("Not connected to NATS")
        conn.publish(subject, message.toByteArray(StandardCharsets.UTF_8))
    }

    fun close() {
        connection?.close()
    }
}
