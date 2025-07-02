package com.relay.realtime.realtimeSDK

import android.content.Context
import android.util.Log
import com.relay.realtime.models.JsonWriter
import com.relay.realtime.models.Pojo
import io.nats.client.Connection
import io.nats.client.ConnectionListener
import io.nats.client.Dispatcher
import io.nats.client.JetStream
import io.nats.client.JetStreamApiException
import io.nats.client.Message
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.PullSubscribeOptions
import io.nats.client.PushSubscribeOptions
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.DeliverPolicy
import io.nats.client.api.StorageType
import io.nats.client.api.StreamConfiguration
import io.nats.client.impl.NatsMessage
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import org.msgpack.core.MessageBufferPacker
import org.msgpack.core.MessagePack
import org.msgpack.core.MessageUnpacker
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Collections
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.atomic.AtomicBoolean


class Realtime(private val context: Context, private val apiKey: String, private val secretKey: String) {

    init {
        require(apiKey.isNotBlank()) { "apiKey must not be empty" }
        require(secretKey.isNotBlank()) { "secretKey must not be empty" }
    }

    private var staging: Boolean = false
    private var debug = false
    private var clientId: String = ""
    private var natsConnection: Connection? = null
    private var jetStream: JetStream? = null

    private val isConnected = AtomicBoolean(false)
    private val sdkListeners = ConcurrentHashMap<String, (String) -> Unit>()
    private val subscribedTopics = CopyOnWriteArraySet<String>()
    private val consumers = ConcurrentHashMap<String, Dispatcher>()
    private val offlineMessages = Collections.synchronizedList(mutableListOf<MutableMap<String, Any?>>())
    private val listeners = ConcurrentHashMap<String, (JSONObject) -> Unit>()
    private val subscriptions = ConcurrentHashMap<String, Dispatcher>()

    private val reservedTopics = setOf(
        "CONNECTED", "RECONNECT", "MESSAGE_RESEND", "DISCONNECTED", "RECONNECTING", "RECONNECTED", "RECONN_FAIL"
    )

    fun init(staging: Boolean, opts: Map<String, Any>?) {
        requireNotNull(opts) { "Options map must not be null" }
        this.staging = staging
        debug = opts["debug"] as? Boolean ?: false
    }

    suspend fun connect(filePath: String) = withContext(Dispatchers.IO) {
        val builder = Options.Builder()
            .authHandler(Nats.credentials(filePath))
            .noEcho()
            .maxReconnects(1200)
            .reconnectWait(Duration.ofMillis(1000))
            .token(apiKey)
            .connectionListener { _, type ->
                when (type) {
                    ConnectionListener.Events.CONNECTED -> {
                        emitSdk("CONNECTED", "CONNECTED")
                    }
                    ConnectionListener.Events.RECONNECTED -> {
                        emitSdk("RECONNECTED", "RECONNECTED")
                        CoroutineScope(Dispatchers.IO).launch { resendOfflineMessages() }
                    }
                    ConnectionListener.Events.DISCONNECTED -> {
                        emitSdk("DISCONNECTED", "DISCONNECTED")
                        offlineMessages.clear()
                    }
                    else -> {}
                }
            }

        for (port in 4221..4223) {
            val host = if (staging) "nats://0.0.0.0:$port" else "nats://api.relay-x.io:$port"
            builder.server(host)
        }

        natsConnection = Nats.connect(builder.build()).also {
            jetStream = it.jetStream()
            clientId = UUID.randomUUID().toString()
            isConnected.set(true)
        }

        emitSdk("CONNECTED", "CONNECTED")
    //        subscribeToTopics()
    }

    suspend fun publish(topic: String, message: Any): Boolean = withContext(Dispatchers.IO) {
        validateTopic(topic)
        validateMessage(message)
        if (reservedTopics.contains(topic)) throw IllegalArgumentException("Reserved SDK topic: $topic")

        val finalTopic = finalTopic(topic)

        ensureStreamExists(topic)

        val json = JSONObject().apply {
            put("client_id", clientId)
            put("id", UUID.randomUUID().toString())
            put("room", topic)
            put("message", message)
            put("start", System.currentTimeMillis() )
        }

        val packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
        packer.writePayload(json.toString().toByteArray(StandardCharsets.UTF_8))
        packer.close()

        if (isConnected.get()) {
            jetStream?.publish(
                NatsMessage.builder()
                    .subject(finalTopic)
                    .data(packer.toByteArray())
                    .build()
            )
            true
        } else {
            offlineMessages.add(mutableMapOf("topic" to topic, "message" to message, "resent" to false))
            false
        }
    }

    fun on(topic: String, listener: (JSONObject) -> Unit) {
        validateTopic(topic)
        listeners[topic] = listener
        val finalTopic = "${getNamespace()}.$topic"
        // Implementation for ephemeral consumer subscribing with listener callback goes here

        if (natsConnection != null && natsConnection?.status == Connection.Status.CONNECTED) {
            val consumerConfig = ConsumerConfiguration.builder()
                .filterSubject(finalTopic)
                .ackPolicy(AckPolicy.Explicit)
                .deliverPolicy(DeliverPolicy.New)
                .build()

            val sub = jetStream?.subscribe(finalTopic, PushSubscribeOptions.builder()
                .configuration(consumerConfig)
                .durable(UUID.randomUUID().toString())
                .build())


            GlobalScope.launch(Dispatchers.IO) {
                while (true) {
                    try {
                        val msg = sub?.nextMessage(Duration.ofSeconds(5)) ?: continue

                        val json = JSONObject(String(msg.data))
                        val msgClientId = json.optString("client_id")
                        val room = json.optString("room")

                        if (msgClientId != natsConnection?.serverInfo?.clientId.toString() && listeners.containsKey(room)) {
                            msg.ack()

                            listeners[room]?.let {
                                it(JSONObject().apply {
                                    put("id", json.getString("id"))
                                    put("message", json.get("message"))
                                })
                            }
                        }
                    } catch (e: Exception) {
                        if (debug) Log.e("RealtimeSDK", "Error handling message: ${e.message}")
                    }
                }
            }

        }
    }

    fun off(topic: String): Boolean {
        validateTopic(topic)
        listeners.remove(topic)

        return consumers.remove(topic)?.let {

//            subscriptions.remove(topic)?.cancel()
            it.unsubscribe(finalTopic(topic))
            subscribedTopics.remove(topic)
            sdkListeners.remove(topic)
            true
        } ?: false
    }

    suspend fun history(topic: String, start: Long, end: Long?): List<String> = withContext(Dispatchers.IO) {
        validateTopic(topic)

        requireNotNull(start) { "Start date cannot be null" }
        if (end != null && end < (start)) throw IllegalArgumentException("End date before start")
        if (!isConnected.get()) return@withContext emptyList()

        val finalTopic = finalTopic(topic)

        val result = mutableListOf<String>()

        val config = ConsumerConfiguration.builder()
            .filterSubject(finalTopic)
            .ackPolicy(AckPolicy.None)
            .deliverPolicy(DeliverPolicy.All)
            .build()
        val opts = PullSubscribeOptions.builder().configuration(config).build()
        val sub = jetStream?.subscribe(finalTopic, opts)

        sub?.pull(100)
        val fetched = sub?.fetch(100, Duration.ofSeconds(10)) ?: return@withContext result


        for (msg in fetched) {
            val unpacker: MessageUnpacker = MessagePack.newDefaultUnpacker(msg.data)
            val bytes = unpacker.readPayload(msg.data.size)

            unpacker.close()
            val json = JSONObject(String(bytes, StandardCharsets.UTF_8))

            val ts = json.optLong("start")
            if (start < ts && (end ?: 0) > ts) {
                result.add(json.toString())
            }
        }
        result
    }

    fun close() {
        try {
            natsConnection?.close()
            isConnected.set(false)
        } catch (e: Exception) {
            if (debug) Log.e("Realtime", "Error on close: ${e.message}")
        }
    }

    private suspend fun resendOfflineMessages() = withContext(Dispatchers.IO) {
        val result = mutableListOf<Map<String, Any?>>()
        for (msg in offlineMessages) {
            val topic = msg["topic"] as? String ?: continue
            val content = msg["message"] ?: continue
            val sent = publish(topic, content)
            msg["resent"] = sent
            result.add(msg)
        }
        offlineMessages.clear()
        sdkListeners["MESSAGE_RESEND"]?.invoke(JSONObject(mapOf("resend" to result)).toString())
    }

    private fun validateTopic(topic: String) {
        require(topic.isNotBlank() && !topic.contains(" ") && !topic.contains("*")) { "Invalid topic" }
    }

    private fun validateMessage(msg: Any) {
        require(msg is String || msg is Number || msg is Map<*, *>) { "Message must be string, number or JSON" }
    }

    private fun finalTopic(topic: String): String =
        "${getNamespace()}.$topic"

    private fun emitSdk(topic: String, message: String) {
        sdkListeners[topic]?.invoke(message)
    }

    private fun ensureStreamExists(topic: String) {
        val streamName = "stream_$topic"

        try {
            natsConnection?.jetStreamManagement()?.addStream(StreamConfiguration.builder()
                .name("stream_$streamName")
                .subjects(listOf(streamName))
                .storageType(StorageType.Memory)
                .build())
        } catch (_: Exception) {}
    }

    private fun getPojo(): Pojo {
        val ppub: Pojo = Pojo()
        ppub.api_key = apiKey
        return ppub
    }


    fun getNamespace(): String? {
        if(natsConnection != null) {
            natsConnection?.let {

                val requestJson = JSONObject()
                requestJson.put("api_key", apiKey)

                val originalPojo: Pojo? = getPojo()
                val originalJson = JsonWriter.toJsonBytes(originalPojo)

                val subject = "accounts.user.get_namespace"
                val timeout = Duration.ofSeconds(20)

                val responseMsg: Message? = it.request(
                    subject,
                    originalJson,
                    timeout
                )

                val responseStr = String(responseMsg?.data ?: byteArrayOf(), Charsets.UTF_8)
                val responseJson = JSONObject(responseStr)
                val responseDataJson = JSONObject(responseJson.getString("data"))

                return responseDataJson.getString("hash")

            } ?: return null
        } else
            return null
    }

    private fun subscribeToTopics() {
        for (topic in subscribedTopics) {
            try {
                val finalTopic = finalTopic(topic)
                ensureStreamExists(topic)
                val config = ConsumerConfiguration.builder()
                    .filterSubject(finalTopic)
                    .ackPolicy(AckPolicy.Explicit)
                    .deliverPolicy(DeliverPolicy.New)
                    .build()
                val options = PushSubscribeOptions.builder().configuration(config).build()
                val dispatcher = natsConnection?.createDispatcher { msg ->
                    try {
                        val unpacker = MessagePack.newDefaultUnpacker(msg.data)
                        val bytes = unpacker.readPayload(msg.data.size)
                        unpacker.close()
                        val json = JSONObject(String(bytes, StandardCharsets.UTF_8))
                        val message = JSONObject().apply {
                            put("id", json.optString("id"))
                            put("message", json.opt("message"))
                        }
                        msg.ack()
                        sdkListeners[topic]?.invoke(message.toString())
                    } catch (e: Exception) {
                        msg.nak()
                        if (debug) Log.e("Realtime", "Error in consumer callback: ${e.message}")
                    }
                } ?: continue
                jetStream?.subscribe(finalTopic, options)
                consumers[topic] = dispatcher
            } catch (e: Exception) {
                if (debug) Log.e("Realtime", "Failed to subscribe to topic '$topic': ${e.message}")
            }
        }
    }
}
