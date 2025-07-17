package com.relay.realtime.realtimeSDK

import android.content.Context
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
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
import java.io.IOException
import java.net.ConnectException
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
import kotlin.collections.mutableListOf
import kotlin.math.log


data class MessageInfo(val client_id: String, val id: String, val room: String, val message: Any, val start: Long)

class Realtime(private val context: Context, private val apiKey: String, private val secretKey: String) {

    init {
        require(apiKey.isNotBlank()) { "apiKey must not be empty" }
        require(secretKey.isNotBlank()) { "secretKey must not be empty" }
    }

    private var staging: Boolean = false
    private var debug = false
    private var opts: Map<String, Any>? = null
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
    private val isReconnecting = AtomicBoolean(false)
    private val reconnStatusSent = AtomicBoolean(false)
    private val connectCalled = AtomicBoolean(false)
    private val latencyHistory = CopyOnWriteArrayList<Map<String, Any>>()
    private var lastLatencyFlushTime = System.currentTimeMillis()
    private var latencyTimerJob: Job? = null

    private var consumer: ConsumerContext? = null;

    private var startZonedDateTime: ZonedDateTime? = null
    fun init(staging: Boolean, opts: Map<String, Any>?) {
        requireNotNull(opts) { "Options map must not be null" }
        this.staging = staging
        debug = opts["debug"] as? Boolean ?: false

        mapper = ObjectMapper(MessagePackFactory()).registerKotlinModule()
    }

    suspend fun connect() = withContext(Dispatchers.IO) {
        if(connectCalled.get()){
            return@withContext
        }

        connectCalled.set(true)

        val credsFile = createNatsCredsFile(context, apiKey, secretKey)

        val builder = Options.Builder()
            .authHandler(Nats.credentials(credsFile.absolutePath))
            .noEcho()
            .maxReconnects(1200)
            .reconnectWait(Duration.ofMillis(1000))
            .token(apiKey)
            .connectionListener { _, type ->
                logCatDebug(type.name)
                when (type) {
                    ConnectionListener.Events.CONNECTED -> {
                        isConnected.set(true)
                        isManuallyDisconnected.set(false)

                        startZonedDateTime = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.of("UTC"))

                        emitSdk("CONNECTED", "CONNECTED")
                    }
                    ConnectionListener.Events.RECONNECTED -> {
                        isConnected.set(true)
                        isReconnecting.set(false)
                        reconnStatusSent.set(false)

                        emitSdk("RECONNECT", "RECONNECTED")

                        CoroutineScope(Dispatchers.IO).launch {
                            deleteConsumer(consumer?.consumerName)
                            subscribeToTopics()

                            resendOfflineMessages()
                        }
                    }
                    ConnectionListener.Events.CLOSED -> {
                        isConnected.set(false)
                        reconnStatusSent.set(false)

                        emitSdk("DISCONNECTED", "DISCONNECTED")

                        offlineMessages.clear()
                        connectCalled.set(false)
                    }
                    ConnectionListener.Events.DISCONNECTED -> {
                        // This actually calls when reconnection attempts are being made
                        isConnected.set(false)
                        isReconnecting.set(true)

                        if(!reconnStatusSent.get()){
                            reconnStatusSent.set(true)

                            // Reinitializing this because we want to get missed messages
                            startZonedDateTime = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.of("UTC"))

                            emitSdk("RECONNECT", "RECONNECTING")
                        }
                    }
                    else -> {}
                }
            }

        // Use actual routable host instead of 0.0.0.0
        for (port in 4221..4223) {
            val host = if (staging) "nats://staging.relay-x.io:$port" else "nats://api.relay-x.io:$port"
            builder.server(host)
        }

        try {
            natsConnection = Nats.connect(builder.build()).also {
                jetStream = it.jetStream()
                clientId = it.serverInfo.clientId.toString()
            }

            namespaceData = getNamespace()
            if (namespaceData != null) {
                namespace = namespaceData?.optString("namespace")
                hash = namespaceData?.optString("hash")
            }

            subscribeToTopics()

            latencyTimerJob = CoroutineScope(Dispatchers.IO).launch {
                while (isActive) {
                    delay(30_000)
                    flushLatencyLog(force = true)
                }
            }

        } catch (e: ConnectException) {
            logCatDebug("Connection failed: ${e.message}")
        } catch (e: IOException) {
            logCatDebug("IO error on connect: ${e.message}")
        } catch (e: Exception) {
            logCatDebug("Unexpected error: ${e.message}")
        }
    }

    suspend fun publish(topic: String, message: Any): Boolean = withContext(Dispatchers.IO) {
        validateTopic(topic)
        validateEmptyMessage(message)
        validateMessage(message)

        if (reservedTopics.contains(topic)) throw IllegalArgumentException("Reserved SDK topic: $topic")

        val finalTopic = finalTopic(topic)
        val sendMessage = MessageInfo(client_id = clientId, id = UUID.randomUUID().toString(), room = topic, message = message, start = System.currentTimeMillis())

        if (!::mapper.isInitialized) {
            mapper = ObjectMapper(MessagePackFactory()).registerKotlinModule()
        }


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

    suspend fun on(topic: String, listener: (JSONObject) -> Unit) : Boolean = withContext(Dispatchers.IO) {
        validateTopic(topic)

        if(listeners.containsKey(topic)){
            return@withContext false
        }

        listeners[topic] = listener

        if(!reservedTopics.contains(topic)){
            subscribedTopics.add(topic)

            if(isConnected.get()){
                startConsumer()
            }
        }

        return@withContext true
    }

    fun off(topic: String) {
        validateTopic(topic)
        listeners.remove(topic)
        subscribedTopics.remove(topic)

        if(subscribedTopics.size == 0){
            deleteConsumer(consumer?.consumerName)
        }
    }

    suspend fun history(topic: String, start: Long, end: Long?): List<Any> = withContext(Dispatchers.IO) {
        validateTopic(topic)
        requireNotNull(start) { "Start date cannot be null" }

        if (end != null && end < start) throw IllegalArgumentException("End date before start")
        if (!isConnected.get()) return@withContext emptyList()

        val finalTopic = finalTopic(topic)
        val result = mutableListOf<Any>()
        val consumerName = "history_consumer_${UUID.randomUUID()}"
        val zonedDateTime = Instant.ofEpochMilli(start).atZone(ZoneId.of("UTC"))

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
            while (true) {
                try {
                    var breakOuter = false

                    logCatDebug("Starting fetch...")

                    val messages = sub?.fetch(1000000, Duration.ofSeconds(2))

                    if(messages?.size == 0 || messages == null){
                        break;
                    }

                    for(msg in messages){
                        val unpacked: MessageInfo = mapper.readValue(msg.data, MessageInfo::class.java)

                        logCatDebug(unpacked.toString())

                        if(end != null){
                            if(unpacked.start > end){
                                breakOuter = true
                                break
                            }
                        }

                        result.add(unpacked.message)
                    }

                    if(breakOuter){
                        break
                    }
                } catch (e: Exception) {
                    logCatDebug("Consumer error [$topic]: ${e.message}")
                    break
                }
            }
        } finally {
            try{
                val streamContext = jetStream?.getStreamContext(getStreamName())
                streamContext?.deleteConsumer(consumerName)
            }catch (e: Exception){
                logCatDebug("ERR => " + e.message)
            }
        }

        result
    }

    fun close() {
        try {
            isManuallyDisconnected.set(true)

            latencyTimerJob?.cancel()
            latencyTimerJob = null

            consumerJobs.values.forEach { it.cancel() }
            consumerJobs.clear()

            natsConnection?.close()
            isConnected.set(false)
        } catch (e: Exception) {
            logCatDebug("Error on close: ${e.message}")
        }
    }

    // ---- Internal Methods -----

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
            logCatDebug("Namespace fetch failed: ${e.message}")
            null
        }
    }

    // Consumers
    private fun startConsumer() {
        if(consumer != null){
            return
        }

        val finalTopic = finalTopic(">")
        val consumerConfig = ConsumerConfiguration.builder()
            .name("consumer_${UUID.randomUUID()}")
            .filterSubject(finalTopic)
            .ackPolicy(AckPolicy.Explicit)
            .startTime(startZonedDateTime)
            .deliverPolicy(DeliverPolicy.ByStartTime)
            .replayPolicy(ReplayPolicy.Instant)
            .build()

        val streamContext = jetStream?.getStreamContext(getStreamName())
        consumer = streamContext?.createOrUpdateConsumer(consumerConfig);

        consumer?.consume{ msg ->
            val topic = stripTopicHash(msg.subject)

            try {
                val receivedTime = System.currentTimeMillis()

                val unpacked = mapper.readValue(msg.data, MessageInfo::class.java)
                val msgClientId = unpacked.client_id

                logCatDebug(unpacked.toString())

                if (msgClientId != clientId) {
                    val topics = getCallbackTopics(topic)
                    logCatDebug(topics.toString())

                    for(top in topics){
                        logCatDebug("Listener => ${listeners.containsKey(top)}")

                        listeners[top]?.invoke(JSONObject().apply {
                            put("id", unpacked.id)
                            put("topic", topic)
                            put("message", unpacked.message)
                        })
                    }

                    msg.ack()

                    logLatency(unpacked.start, receivedTime)
                }
            } catch (e: Exception) {
                logCatDebug("Consumer error [$topic]: ${e.message}")
            }
        }
    }

    private fun deleteConsumer(consumerName: String?){
        logCatDebug("Consumer to delete => " + consumerName)

        if(consumer != null){
            logCatDebug("Deleting consumer....")
            try{
                val streamContext = jetStream?.getStreamContext(getStreamName())
                streamContext?.deleteConsumer(consumerName)
            }catch (e: Exception){
                logCatDebug("ERR => " + e.message)
            }

            consumer = null
            logCatDebug("Consumer deleted => ${consumerName}")
        }
    }

    private fun subscribeToTopics() {
        if (subscribedTopics.size > 0) {
            startConsumer()
        }
    }

    // ---- Latency Logging ----
    private fun logLatency(sentTime: Long, receivedTime: Long) {
        val latency = receivedTime - sentTime
        latencyHistory.add(mapOf("latency" to latency, "timestamp" to receivedTime))
        flushLatencyLog()
    }

    private fun flushLatencyLog(force: Boolean = false) {
        val now = System.currentTimeMillis()
        if (latencyHistory.isEmpty()) return

        val shouldFlush = latencyHistory.size >= 100 || force || (now - lastLatencyFlushTime) >= 30_000
        if (!shouldFlush) return

        println("latencyHistory.toList(): " + latencyHistory.toList())
//        val payload = JSONObject().apply {
//            put("timezone", TimeZone.getDefault().id)
//            put("history", latencyHistory.toList())
//        }


        val payload = mapOf(
            "timezone" to TimeZone.getDefault().id,
            "history" to latencyHistory.toList()
        )
        println("payload: " + payload)

        val originalJson = JsonWriter.toJsonBytes(payload)
        natsConnection?.request("accounts.user.log_latency", originalJson, Duration.ofSeconds(5))

        if (debug) Log.d("Realtime", "Published latency log with ${latencyHistory.size} entries")

        latencyHistory.clear()
        lastLatencyFlushTime = now
    }


    // ---- Connection Recovery Methods ----
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

        listeners["MESSAGE_RESEND"]?.invoke(JSONObject(mapOf("data" to result)))
    }

    // ---- Utility Function ----
    private fun getCallbackTopics(topic: String) : List<String> {
        val validTopics = mutableListOf<String>()

        val topicPatterns = listeners.keys;

        for(pattern in topicPatterns){
            if(reservedTopics.contains(pattern)){
                continue;
            }

            val match = topicPatternMatcher(pattern, topic);

            if(match){
                validTopics.add(pattern)
            }
        }

        return validTopics;

    }

    fun topicPatternMatcher(patternA: String, patternB: String): Boolean {
        val a = patternA.split(".")
        val b = patternB.split(".")

        var i = 0
        var j = 0

        var starAi = -1
        var starAj = -1
        var starBi = -1
        var starBj = -1

        while (i < a.size || j < b.size) {
            val tokA = a.getOrNull(i)
            val tokB = b.getOrNull(j)

            val singleWildcard =
                (tokA == "*" && j < b.size) ||
                        (tokB == "*" && i < a.size)

            if ((tokA != null && tokA == tokB) || singleWildcard) {
                i++
                j++
                continue
            }

            // Handle multi-token wildcard ">" — must be final
            if (tokA == ">") {
                if (i != a.lastIndex) return false // '>' must be at the end
                if (j >= b.size) return false       // must consume at least one token
                starAi = i++
                starAj = ++j
                continue
            }
            if (tokB == ">") {
                if (j != b.lastIndex) return false
                if (i >= a.size) return false
                starBi = j++
                starBj = ++i
                continue
            }

            // Backtrack if previous '>' was seen
            if (starAi != -1) {
                j = ++starAj
                continue
            }
            if (starBi != -1) {
                i = ++starBj
                continue
            }

            return false // No match possible
        }

        return true
    }

    fun validateTopic(topic: String) {
        val topicNotNull = !topic.isBlank()

        val topicRegex = Regex("^(?!.*\\\$)(?:[A-Za-z0-9_*~-]+(?:\\.[A-Za-z0-9_*~-]+)*(?:\\.>)?|>)\$")

        val spaceStarCheck = !topic.contains(" ") && topicRegex.matches(topic)

        require(spaceStarCheck && topicNotNull) { "Invalid topic" }
    }

    private fun validateMessage(msg: Any) {
        require(msg is String || msg is Number || msg is Map<*, *>) { "Message must be string, number or JSON" }
    }

    private fun validateEmptyMessage(msg: Any) {
        require(msg != null) { "Message must not be null or empty" }
    }

    private fun finalTopic(topic: String): String = "$hash.$topic"

    private fun emitSdk(topic: String, message: String) {
        listeners[topic]?.invoke(JSONObject().apply {
            put("status", message)
        })
    }

    private fun getRequestBody(): RequestBody {
        val requestBody = RequestBody()
        requestBody.api_key = apiKey
        return requestBody
    }

    private fun logCatDebug(message: String) {
        if (debug) Log.i("RealtimeSDK", message)
    }

    fun checkIsConnected(): Boolean {
        return  isConnected.get()
    }

    fun listenersList(): ConcurrentHashMap<String, (JSONObject) -> Unit> {
        return listeners
    }

    fun flushLatencyLogPublic(force: Boolean) {
        flushLatencyLog(force)
    }

    fun getStaging(): Boolean {
        return staging
    }

    fun getOpts(): Map<String, Any>? {
        return opts
    }

    fun getStreamName(): String = "${namespace}_stream"

    fun stripTopicHash(topic: String): String = topic.replace("${hash}.", "")
}
