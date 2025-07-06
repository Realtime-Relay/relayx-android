package com.relay.realtime.realtimeSDK

import android.content.Context
import android.util.Log
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.relay.realtime.models.JsonWriter
import com.relay.realtime.models.RequestBody
import com.relay.realtime.realtimeSDK.Utils.createNatsCredsFile
import io.nats.client.*
import io.nats.client.api.*
import io.nats.client.impl.NatsMessage
import kotlinx.coroutines.*
import org.json.JSONObject
import org.msgpack.core.MessageBufferPacker
import org.msgpack.core.MessagePack
import org.msgpack.jackson.dataformat.MessagePackFactory
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.atomic.AtomicBoolean


data class MessageInfo(val client_id: String, val id: String, val room: String, val message: Any, val start: Long)

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
    private var namespaceData: JSONObject? = null
    private var namespace: String? = null
    private var hash: String? = null

    private val isConnected = AtomicBoolean(false)
    private val sdkListeners = ConcurrentHashMap<String, (Any) -> Unit>()
    private val subscribedTopics = CopyOnWriteArraySet<String>()
    private val consumers = ConcurrentHashMap<String, Dispatcher>()
    private val consumerJobs = ConcurrentHashMap<String, Job>()
    private val offlineMessages = Collections.synchronizedList(mutableListOf<MutableMap<String, Any?>>())
    private val listeners = ConcurrentHashMap<String, (JSONObject) -> Unit>()
    private val isManuallyDisconnected = AtomicBoolean(false)

    private lateinit var mapper: ObjectMapper
    private val reservedTopics = setOf("CONNECTED", "RECONNECT", "MESSAGE_RESEND", "DISCONNECTED", "RECONNECTING", "RECONNECTED", "RECONN_FAIL")
    private val ephemeralConsumers = ConcurrentHashMap<String, String>()
    private val isReconnecting = AtomicBoolean(false)
    private val latencyHistory = CopyOnWriteArrayList<Map<String, Any>>()

    fun init(staging: Boolean, opts: Map<String, Any>?) {
        requireNotNull(opts) { "Options map must not be null" }
        this.staging = staging
        debug = opts["debug"] as? Boolean ?: false

        mapper = ObjectMapper(MessagePackFactory()).registerKotlinModule()
    }

    suspend fun connect() = withContext(Dispatchers.IO) {
        val credsFile = createNatsCredsFile(context, apiKey, secretKey)

        val builder = Options.Builder()
            .authHandler(Nats.credentials(credsFile.absolutePath))
            .noEcho()
            .maxReconnects(1200)
            .reconnectWait(Duration.ofMillis(1000))
            .token(apiKey)
            .connectionListener { _, type ->
                when (type) {
                    ConnectionListener.Events.CONNECTED -> {
                        isManuallyDisconnected.set(false)
                        emitSdk("CONNECTED", "CONNECTED")
                    }
                    ConnectionListener.Events.RECONNECTED -> {
                        if (!isManuallyDisconnected.get()) {
                            isReconnecting.set(false)
                            emitSdk("RECONNECTED", "RECONNECTED")
                            CoroutineScope(Dispatchers.IO).launch { resendOfflineMessages() }
                        }
                    }
                    ConnectionListener.Events.DISCONNECTED -> {
                        if (!isManuallyDisconnected.get()) {
                            if (isReconnecting.compareAndSet(false, true)) {
                                emitSdk("RECONNECTING", "RECONNECTING")
                            }
                            emitSdk("RECONNECT", "RECONNECTING")
                        }
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
            clientId = it.serverInfo.clientId.toString()
            isConnected.set(true)
        }

        namespaceData = getNamespace()
        if (namespaceData != null) {
            namespace = namespaceData?.optString("namespace")
            hash = namespaceData?.optString("hash")
        }

        emitSdk("CONNECTED", "CONNECTED")
        subscribeToTopics()
    }

    suspend fun publish(topic: String, message: Any): Boolean = withContext(Dispatchers.IO) {
        validateTopic(topic)
        validateEmptyMessage(message)
        validateMessage(message)

        if (reservedTopics.contains(topic)) throw IllegalArgumentException("Reserved SDK topic: $topic")

        val finalTopic = finalTopic(topic)
        val sendMessage = MessageInfo(client_id = clientId, id = UUID.randomUUID().toString(), room = topic, message = message, start = System.currentTimeMillis())
        val packed: ByteArray = mapper.writeValueAsBytes(sendMessage)
        val packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
        packer.writePayload(packed)
        packer.close()

        if (isConnected.get()) {
            jetStream?.publish(NatsMessage.builder().subject(finalTopic).data(packer.toByteArray()).build())
            true
        } else {
            offlineMessages.add(mutableMapOf("topic" to topic, "message" to message, "resent" to false))
            false
        }
    }

    fun on(topic: String, listener: (JSONObject) -> Unit) {
        validateTopic(topic)
        if (subscribedTopics.contains(topic)) return
        listeners[topic] = listener
        if (natsConnection?.status == Connection.Status.CONNECTED) {
            val consumerName = "consumer_${UUID.randomUUID()}"
            ephemeralConsumers[topic] = consumerName
            startConsumer(topic)
        }
    }

    fun off(topic: String): Boolean {
        validateTopic(topic)
        listeners.remove(topic)
        consumerJobs.remove(topic)?.cancel()
        val removed = consumers.remove(topic)
        ephemeralConsumers.remove(topic)?.let { name ->
            try {
                natsConnection?.jetStreamManagement()?.deleteConsumer(namespace, name)
            } catch (e: Exception) {
                if (debug) Log.e("Realtime", "Failed to delete ephemeral consumer: ${e.message}")
            }
        }
        subscribedTopics.remove(topic)
        sdkListeners.remove(topic)
        return removed != null
    }


    suspend fun history(topic: String, start: Long, end: Long?): List<Any> = withContext(Dispatchers.IO) {
        validateTopic(topic)

        requireNotNull(start) { "Start date cannot be null" }
        if (end != null && end < (start)) throw IllegalArgumentException("End date before start")
        if (!isConnected.get()) return@withContext emptyList()

        val finalTopic = finalTopic(topic)

        val result = mutableListOf<Any>()
        val consumerName = "history_consumer_${UUID.randomUUID()}"

        val zonedDateTime: ZonedDateTime = Instant.ofEpochMilli(start)
            .atZone(ZoneId.systemDefault()) // or use ZoneId.of("UTC") if needed


        val config = ConsumerConfiguration.builder()
            .name(consumerName)
            .filterSubject(finalTopic)
            .startTime(zonedDateTime)
            .ackPolicy(AckPolicy.None)
            .deliverPolicy(DeliverPolicy.ByStartTime)
            .replayPolicy(ReplayPolicy.Instant)
            .build()
        val opts = PullSubscribeOptions.builder().configuration(config).build()
        val sub = jetStream?.subscribe(finalTopic, opts)

        try {
            sub?.pull(100)
            val fetched = sub?.fetch(100, Duration.ofSeconds(10)) ?: return@withContext result

            for (msg in fetched) {
                val unpacked: MessageInfo = mapper.readValue(msg.data, MessageInfo::class.java) // ➜ back to object
                if (start < unpacked.start && (end ?: System.currentTimeMillis()) > unpacked.start) {
                    result.add(unpacked.message)
                }
            }
        } finally {
            try {
                val jsm = natsConnection?.jetStreamManagement()
                if (namespace != null) {
                    jsm?.deleteConsumer(namespace, consumerName)
                } else if (debug) {
                    Log.e("Realtime", "Stream not found for subject: $finalTopic")
                }
            } catch (e: Exception) {
                if (debug) Log.e("Realtime", "Failed to delete consumer: ${e.message}")
            }
        }
        result
    }

    fun close() {
        try {
            isManuallyDisconnected.set(true)
            consumerJobs.values.forEach { it.cancel() }
            consumerJobs.clear()
            natsConnection?.close()
            isConnected.set(false)
        } catch (e: Exception) {
            if (debug) Log.e("Realtime", "Error on close: ${e.message}")
        }
    }

    private fun startConsumer(topic: String) {
        val finalTopic = finalTopic(topic)
        val consumerConfig = ConsumerConfiguration.builder()
            .name(ephemeralConsumers[topic])
            .filterSubject(finalTopic)
            .ackPolicy(AckPolicy.Explicit)
            .deliverPolicy(DeliverPolicy.New)
            .replayPolicy(ReplayPolicy.Instant)
            .build()
        val sub = jetStream?.subscribe(finalTopic, PushSubscribeOptions.builder().configuration(consumerConfig).build())

        val job = CoroutineScope(Dispatchers.IO).launch {
            while (isActive && natsConnection?.status == Connection.Status.CONNECTED) {
                try {
                    val msg = sub?.nextMessage(Duration.ofSeconds(5)) ?: continue
                    val unpacked = mapper.readValue(msg.data, MessageInfo::class.java)
                    val msgClientId = unpacked.client_id
                    val room = unpacked.room
                    if (msgClientId != clientId && listeners.containsKey(room)) {
                        msg.ack()
                        listeners[room]?.invoke(JSONObject().apply {
                            put("id", unpacked.id)
                            put("message", unpacked.message)
                        })
                        logLatency(unpacked.start)
                    }
                } catch (e: Exception) {
                    if (debug) Log.e("Realtime", "Consumer error [$topic]: ${e.message}")
                    break
                }
            }
            if (debug) Log.d("Realtime", "Consumer loop for $topic exited.")
        }
        consumerJobs[topic] = job
    }

    private fun logLatency(sentTime: Long) {
        val receivedTime = System.currentTimeMillis()
        val latency = receivedTime - sentTime
        val timezone = TimeZone.getDefault().id

        latencyHistory.add(mapOf("latency" to latency, "timestamp" to receivedTime))

        if (latencyHistory.size >= 100) {
            val payload = JSONObject().apply {
                put("timezone", timezone)
                put("history", latencyHistory.toList())
            }

            try {
                val packed = mapper.writeValueAsBytes(payload)
                val packer = MessagePack.newDefaultBufferPacker()
                packer.writePayload(packed)
                packer.close()

                jetStream?.publish(
                    NatsMessage.builder()
                        .subject("accounts.user.log_latency")
                        .data(packer.toByteArray())
                        .build()
                )
            } catch (e: Exception) {
                if (debug) Log.e("Realtime", "Failed to publish latency log: ${e.message}")
            } finally {
                latencyHistory.clear()
            }
        }
    }


    private fun subscribeToTopics() {
        for (topic in subscribedTopics) {
            startConsumer(topic)
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
        sdkListeners["MESSAGE_RESEND"]?.invoke(JSONObject(mapOf("resend" to result)))
    }

    private fun validateTopic(topic: String) {
        require(!topic.isBlank() && !topic.contains(" ") && !topic.contains("*")) { "Invalid topic" }
    }

    private fun validateMessage(msg: Any) {
        require(msg is String || msg is Number || msg is Map<*, *>) { "Message must be string, number or JSON" }
    }

    private fun validateEmptyMessage(msg: Any) {
        require(msg != null) { "Message must not be null or empty" }
    }

    private fun finalTopic(topic: String): String = "$hash.$topic"

    private fun emitSdk(topic: String, message: String) {
        sdkListeners[topic]?.invoke(message)
    }

    private fun getRequestBody(): RequestBody {
        val requestBody = RequestBody()
        requestBody.api_key = apiKey
        return requestBody
    }

    private fun getNamespace(): JSONObject? {
        return try {
            val originalJson = JsonWriter.toJsonBytes(getRequestBody())
            val responseMsg = natsConnection?.request("accounts.user.get_namespace", originalJson, Duration.ofSeconds(20))
            val responseStr = String(responseMsg?.data ?: byteArrayOf(), StandardCharsets.UTF_8)
            val responseJson = JSONObject(responseStr)
            if (responseJson.getString("status") == "NAMESPACE_RETRIEVE_SUCCESS") {
                JSONObject(responseJson.getString("data"))
            } else null
        } catch (e: Exception) {
            if (debug) Log.e("Realtime", "Namespace fetch failed: ${e.message}")
            null
        }
    }
}
