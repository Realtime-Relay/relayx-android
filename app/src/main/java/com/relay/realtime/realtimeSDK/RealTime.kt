package com.relay.realtime.realtimeSDK

import android.content.Context
import android.util.Log
import com.relay.realtime.R
import io.nats.client.*
import io.nats.client.api.*
import io.nats.client.impl.NatsMessage
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import org.msgpack.core.MessagePack
import org.msgpack.core.MessageUnpacker
import org.msgpack.core.MessageBufferPacker
import java.io.File
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
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

    private val reservedTopics = setOf(
        "CONNECTED", "RECONNECT", "MESSAGE_RESEND", "DISCONNECTED", "RECONNECTING", "RECONNECTED", "RECONN_FAIL"
    )

    fun init(staging: Boolean, opts: Map<String, Any>?) {
        requireNotNull(opts) { "Options map must not be null" }
        this.staging = staging
        debug = opts["debug"] as? Boolean ?: false
    }

    fun copyAssetToCache(context: Context, assetFileName: String): String {
        val file = File(context.cacheDir, assetFileName)
        if (!file.exists()) {
            context.assets.open(assetFileName).use { input ->
                FileOutputStream(file).use { output ->
                    input.copyTo(output)
                }
            }
        }
        return file.absolutePath
    }


    suspend fun connect() = withContext(Dispatchers.IO) {
        val credsPath = copyAssetToCache(context, "admin.creds")

        val builder = Options.Builder()
            .credentialPath(credsPath)
            .noEcho()
            .maxReconnects(1200)
            .reconnectWait(Duration.ofMillis(1000))
            .token(apiKey)
            .connectionListener { _, type ->
                when (type) {
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

        subscribeToTopics()
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
            put("start", Instant.now().epochSecond)
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

    suspend fun on(topic: String, listener: (String) -> Unit) = withContext(Dispatchers.IO) {
        validateTopic(topic)
        val finalTopic = finalTopic(topic)

        val config = ConsumerConfiguration.builder()
            .filterSubject(finalTopic)
            .ackPolicy(AckPolicy.Explicit)
            .deliverPolicy(DeliverPolicy.New)
            .build()

        val options = PushSubscribeOptions.builder()
            .configuration(config)
            .build()

        val sub = jetStream?.subscribe(finalTopic, options) ?: return@withContext

        val dispatcher = natsConnection?.createDispatcher { msg ->
            val unpacker: MessageUnpacker = MessagePack.newDefaultUnpacker(msg.data)
            val bytes = unpacker.readPayload(msg.data.size)
            unpacker.close()
            val json = JSONObject(String(bytes, StandardCharsets.UTF_8))
            if (json.optString("client_id") != clientId && json.optString("room") == topic) {
                listener(json.toString())
            }
            msg.ack()
        } ?: return@withContext

        natsConnection?.flush(Duration.ofSeconds(1))

        consumers[topic] = dispatcher
        subscribedTopics.add(topic)
        sdkListeners[topic] = listener
    }

    fun off(topic: String): Boolean {
        validateTopic(topic)
        return consumers.remove(topic)?.let {
            it.unsubscribe(finalTopic(topic))
            subscribedTopics.remove(topic)
            sdkListeners.remove(topic)
            true
        } ?: false
    }

    suspend fun history(topic: String, start: LocalDateTime, end: LocalDateTime?): List<String> = withContext(Dispatchers.IO) {
        validateTopic(topic)

        println("Connection: " +isConnected.get())
        requireNotNull(start) { "Start date cannot be null" }
        if (end != null && end.isBefore(start)) throw IllegalArgumentException("End date before start")
        if (!isConnected.get()) return@withContext emptyList()

        val finalTopic = finalTopic(topic)
        val from = start.toEpochSecond(ZoneOffset.UTC)
        val to = (end ?: LocalDateTime.now()).toEpochSecond(ZoneOffset.UTC)
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
            if (ts in from..to) result.add(json.toString())
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
            val jsm = natsConnection?.jetStreamManagement() ?: return
            jsm.getStreamInfo(streamName)
        } catch (e: JetStreamApiException) {
            val config = StreamConfiguration.builder()
                .name(streamName)
                .subjects(finalTopic(topic))
                .storageType(StorageType.File)
                .build()
            natsConnection?.jetStreamManagement()?.addStream(config)
        }
    }

    private fun getNamespace(): String? {
        val requestSubject = "account.user.get_namespace"
        val payload = JSONObject().put("api_key", apiKey).toString().toByteArray(StandardCharsets.UTF_8)
        val response = natsConnection?.request(requestSubject, payload, Duration.ofSeconds(2)) ?: return null
        val json = JSONObject(String(response.data, StandardCharsets.UTF_8))
        return json.optString("namespace", null)
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
