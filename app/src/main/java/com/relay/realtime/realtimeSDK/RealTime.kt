package com.relay.realtime.realtimeSDK

import android.content.Context
import android.util.Log
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.relay.realtime.models.JsonWriter
import com.relay.realtime.models.RequestBody
import com.relay.realtime.realtimeSDK.Utils.createNatsCredsFile
import io.nats.client.Connection
import io.nats.client.ConnectionListener
import io.nats.client.Dispatcher
import io.nats.client.JetStream
import io.nats.client.Message
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.PullSubscribeOptions
import io.nats.client.PushSubscribeOptions
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.DeliverPolicy
import io.nats.client.api.ReplayPolicy
import io.nats.client.impl.NatsMessage
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import org.msgpack.core.MessageBufferPacker
import org.msgpack.core.MessagePack
import org.msgpack.jackson.dataformat.MessagePackFactory
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Collections
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.atomic.AtomicBoolean

data class MessageInfo(val client_id: String, val id: String, val room: String, val message: Any, val start: Long)
data class ResendMessageInfo(val topic: String, val message: Any, val resent: Boolean)



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
    private val sdkListeners = ConcurrentHashMap<String, (String) -> Unit>()
    private val subscribedTopics = CopyOnWriteArraySet<String>()
    private val consumers = ConcurrentHashMap<String, Dispatcher>()
    private val offlineMessages = Collections.synchronizedList(mutableListOf<MutableMap<String, Any?>>())
    private val listeners = ConcurrentHashMap<String, (JSONObject) -> Unit>()
    private val subscriptions = ConcurrentHashMap<String, Dispatcher>()

    private lateinit var mapper: ObjectMapper
    private val reservedTopics = setOf(
        "CONNECTED", "RECONNECT", "MESSAGE_RESEND", "DISCONNECTED", "RECONNECTING", "RECONNECTED", "RECONN_FAIL"
    )
    private val ephemeralConsumers = ConcurrentHashMap<String, String>()
    private val isReconnecting = AtomicBoolean(false)


    fun init(staging: Boolean, opts: Map<String, Any>?) {
        requireNotNull(opts) { "Options map must not be null" }
        this.staging = staging
        debug = opts["debug"] as? Boolean ?: false

        mapper = ObjectMapper(MessagePackFactory())
            .registerKotlinModule() // adds data-class & null-safety support
    }

    suspend fun connect() = withContext(Dispatchers.IO) {
        val credsFile = createNatsCredsFile(context, Utils.API_KEY, Utils.SECRET_KEY)

        val builder = Options.Builder()
            .authHandler(Nats.credentials(credsFile.absolutePath))
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
                        isReconnecting.set(false)
                        emitSdk("RECONNECTED", "RECONNECTED")
                        CoroutineScope(Dispatchers.IO).launch { resendOfflineMessages() }
                    }
                    ConnectionListener.Events.DISCONNECTED -> {
                        if (isReconnecting.compareAndSet(false, true)) {
                            emitSdk("RECONNECTING", "RECONNECTING")
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
        namespace = namespaceData?.optString("namespace")
        hash = namespaceData?.optString("hash")

        emitSdk("CONNECTED", "CONNECTED")
            subscribeToTopics()
    }

    suspend fun publish(topic: String, message: Any, isRentMessage: Boolean = false): Boolean = withContext(Dispatchers.IO) {
        validateTopic(topic)
        validateEmptyMessage(message)
        validateMessage(message)
        if (reservedTopics.contains(topic)) throw IllegalArgumentException("Reserved SDK topic: $topic")

        val finalTopic = finalTopic(topic)

        var sendMessage = if(isRentMessage) {
            ResendMessageInfo(
                topic = topic,
                message = message,
                resent = true
            )
        } else {
             MessageInfo(
                client_id = clientId,
                id = UUID.randomUUID().toString(),
                room = topic,
                message = message,
                start = System.currentTimeMillis()
            )
        }


        val packed: ByteArray = mapper.writeValueAsBytes(sendMessage) // ➜ send/store

        val packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
        packer.writePayload(packed)
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

        if (subscribedTopics.contains(topic)) return
        listeners[topic] = listener

        if (natsConnection != null && natsConnection?.status == Connection.Status.CONNECTED) {
            val consumerName = "consumer_${UUID.randomUUID()}"
            ephemeralConsumers[topic] = consumerName

            startConsumer(topic)
        }
    }

    fun off(topic: String): Boolean {
        validateTopic(topic)
        listeners.remove(topic)
        val removed = consumers.remove(topic)
        val consumerName = ephemeralConsumers.remove(topic)
        val finalTopic = finalTopic(topic)

        if (consumerName != null) {
            try {
                val jsm = natsConnection?.jetStreamManagement()
                val streamName = jsm?.streamNames?.firstOrNull { stream ->
                    try {
                        val info = jsm.getStreamInfo(stream)
                        info.config.subjects.any { subject -> finalTopic.contains(subject) }
                    } catch (e: Exception) {
                        false
                    }
                }
                if (streamName != null) {
                    jsm?.deleteConsumer(streamName, consumerName)
                }
            } catch (e: Exception) {
                if (debug) Log.e("Realtime", "Failed to delete ephemeral consumer: ${e.message}")
            }
        }

        subscribedTopics.remove(topic)
        sdkListeners.remove(topic)
        return removed != null
    }

//    fun off(topic: String): Boolean {
//        validateTopic(topic)
//        listeners.remove(topic)
//
//        return consumers.remove(topic)?.let {
//
////            subscriptions.remove(topic)?.cancel()
//            it.unsubscribe(finalTopic(topic))
//            subscribedTopics.remove(topic)
//            sdkListeners.remove(topic)
//            true
//        } ?: false
//    }

    suspend fun history(topic: String, start: Long, end: Long?): List<Any> = withContext(Dispatchers.IO) {
        validateTopic(topic)

        requireNotNull(start) { "Start date cannot be null" }
        if (end != null && end < (start)) throw IllegalArgumentException("End date before start")
        if (!isConnected.get()) return@withContext emptyList()

        val finalTopic = finalTopic(topic)

        val result = mutableListOf<Any>()
        val consumerName = "history_consumer_${UUID.randomUUID()}"

        val config = ConsumerConfiguration.builder()
            .name(consumerName)
            .filterSubject(finalTopic)
            .ackPolicy(AckPolicy.None)
            .deliverPolicy(DeliverPolicy.All)
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

                val streamName = jsm?.streamNames?.firstOrNull { stream ->
                    try {
                        val info = jsm.getStreamInfo(stream)
                        info.config.subjects.any { subject -> finalTopic.contains(subject) }

                        true
                    } catch (e: Exception) {
                        false
                    }
                }

                if (streamName != null) {
                    jsm.deleteConsumer(streamName, consumerName)
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
            val sent = publish(topic, content, true)
            msg["resent"] = sent
            result.add(msg)
        }
        offlineMessages.clear()
        sdkListeners["MESSAGE_RESEND"]?.invoke(JSONObject(mapOf("resend" to result)).toString())
    }

    private fun validateTopic(topic: String) {
        require(!topic.isNullOrBlank() && !topic.isNullOrEmpty() && topic.isNotBlank() && !topic.contains(" ") && !topic.contains("*")) { "Invalid topic" }
    }

    private fun validateMessage(msg: Any) {
        require(msg is String || msg is Number || msg is Map<*, *>) { "Message must be string, number or JSON" }
    }

    private fun validateEmptyMessage(msg: Any) {
        require(!msg.toString().isNullOrEmpty() || !msg.toString().isNullOrBlank()) { "Message must not be null or empty" }
    }

    private fun finalTopic(topic: String): String =
        "${namespace}.$topic"

    private fun emitSdk(topic: String, message: String) {
        sdkListeners[topic]?.invoke(message)
    }

    private fun getRequestBody(): RequestBody {
        val requestBody: RequestBody = RequestBody()
        requestBody.api_key = apiKey
        return requestBody
    }


    fun getNamespace(): JSONObject? {
        if(natsConnection != null) {
            natsConnection?.let {

                val requestJson = JSONObject()
                requestJson.put("api_key", apiKey)

                val originalRequestBody: RequestBody? = getRequestBody()
                val originalJson = JsonWriter.toJsonBytes(originalRequestBody)

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

                return responseDataJson

            } ?: return null
        } else
            return null
    }

    private fun startConsumer(topic: String) {
        val finalTopic = finalTopic(topic)
        val consumerConfig = ConsumerConfiguration.builder()
            .filterSubject(finalTopic)
            .ackPolicy(AckPolicy.Explicit)
            .deliverPolicy(DeliverPolicy.New)
            .replayPolicy(ReplayPolicy.Instant)
            .build()

        val sub = jetStream?.subscribe(finalTopic, PushSubscribeOptions.builder().configuration(consumerConfig).build())

        GlobalScope.launch(Dispatchers.IO) {
            while (true) {
                try {
                    val msg = sub?.nextMessage(Duration.ofSeconds(5)) ?: continue
                    val unpacked: MessageInfo = mapper.readValue(msg.data, MessageInfo::class.java)
                    val msgClientId = unpacked.client_id
                    val room = unpacked.room
                    if (msgClientId != natsConnection?.serverInfo?.clientId.toString() && listeners.containsKey(room)) {
                        msg.ack()
                        listeners[room]?.invoke(JSONObject().apply {
                            put("id", unpacked.id)
                            put("message", unpacked.message)
                        })
                    }
                } catch (e: Exception) {
                    if (debug) Log.e("Realtime", "Error in consumer loop: ${e.message}")
                }
            }
        }
    }

    private fun subscribeToTopics() {
        for (topic in subscribedTopics) {
            startConsumer(topic)
        }
    }
}
