package com.realtime.relay.realtimeSDK

import android.content.Context
import android.util.Log
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.realtime.relay.models.JsonWriter
import com.realtime.relay.models.LatencyBody
import com.realtime.relay.models.RequestBody
import com.realtime.relay.realtimeSDK.Utils.createNatsCredsFile
import io.nats.client.*
import io.nats.client.api.*
import io.nats.client.impl.NatsMessage
import kotlinx.coroutines.*
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


data class MessageInfo(val client_id: String, val id: String, val room: String, val message: Any, val start: Long)

class Realtime @JvmOverloads constructor(
private val context: Context,
private val apiKey: String,
private val secretKey: String,
private val callbackDispatcher: CoroutineDispatcher = Dispatchers.Main.immediate
) {

    companion object{
        val CONNECTED = "CONNECTED"
        var RECONNECT = "RECONNECT"
        val MESSAGE_RESEND = "MESSAGE_RESEND"
        val DISCONNECTED = "DISCONNECTED"
        val RECONNECTING = "RECONNECTING"
        val RECONNECTED = "RECONNECTED"
        val RECONN_FAIL = "RECONN_FAIL"
    }

    private var staging: Boolean = false
    private var debug = false
    private var opts: Map<String, Any>? = null
    private var clientId: String = ""
    private var natsConnection: Connection? = null
    private var jetStream: JetStream? = null
    private var namespaceData: JsonObject? = null
    private var namespace: String? = null
    private var hash: String? = null

    private val isConnected = AtomicBoolean(false)
    private val sdkListeners = ConcurrentHashMap<String, (Any) -> Unit>()
    private val subscribedTopics = CopyOnWriteArraySet<String>()
    private val consumerMap = ConcurrentHashMap<String, ConsumerContext>()
    private val consumerJobs = ConcurrentHashMap<String, Job>()
    private val offlineMessages = Collections.synchronizedList(mutableListOf<MutableMap<String, Any?>>())
    private val listeners = ConcurrentHashMap<String, (JsonObject) -> Unit>()
    private val isManuallyDisconnected = AtomicBoolean(false)

    private lateinit var mapper: ObjectMapper
    private val reservedTopics = setOf(CONNECTED, RECONNECT, MESSAGE_RESEND, DISCONNECTED, RECONNECTING, RECONNECTED, RECONN_FAIL)
    private val isReconnecting = AtomicBoolean(false)
    private val reconnStatusSent = AtomicBoolean(false)
    private val connectCalled = AtomicBoolean(false)
    private val latencyHistory = CopyOnWriteArrayList<JsonObject>()
    private var lastLatencyFlushTime = System.currentTimeMillis()
    private var latencyTimerJob: Job? = null

    private var startZonedDateTime: ZonedDateTime? = null

    private val ioScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val callbackScope = CoroutineScope(SupervisorJob() + callbackDispatcher)

    init {
        require(apiKey.isNotBlank()) { "apiKey must not be empty" }
        require(secretKey.isNotBlank()) { "secretKey must not be empty" }
    }

    fun init(staging: Boolean, opts: Map<String, Any>?) {
        requireNotNull(opts) { "Options map must not be null" }

        this.opts = opts

        this.staging = staging
        debug = opts["debug"] as? Boolean ?: false

        mapper = ObjectMapper(MessagePackFactory()).registerKotlinModule()
    }

    suspend fun connect() = withContext(Dispatchers.IO) {
        if(connectCalled.get()){
            return@withContext
        }

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
                        isManuallyDisconnected.set(false)
                        isConnected.set(true)

                        namespaceData = getNamespace()
                        logCatDebug(namespaceData.toString())
                        if (namespaceData != null) {
                            namespace = namespaceData?.get("namespace")?.asString
                            hash = namespaceData?.get("hash")?.asString
                        }

                        subscribeToTopics()

                        connectCalled.set(true)

                        emitSdk(CONNECTED, CONNECTED)

                        latencyTimerJob = CoroutineScope(Dispatchers.IO).launch {
                            while (isActive) {
                                delay(30_000)
                                flushLatencyLog(force = true)
                            }
                        }
                    }
                    ConnectionListener.Events.RECONNECTED -> {
                        isConnected.set(true)
                        isReconnecting.set(false)
                        reconnStatusSent.set(false)

                        emitSdk(RECONNECT, RECONNECTED)

                        CoroutineScope(Dispatchers.IO).launch {
                            resubscribeToTopics()

                            resendOfflineMessages()
                        }
                    }
                    ConnectionListener.Events.CLOSED -> {
                        isConnected.set(false)
                        reconnStatusSent.set(false)
                        isReconnecting.set(false)

                        offlineMessages.clear()
                        connectCalled.set(false)

                        startZonedDateTime = null

                        emitSdk(DISCONNECTED, DISCONNECTED)
                    }
                    ConnectionListener.Events.DISCONNECTED -> {
                        // This actually calls when reconnection attempts are being made
                        isConnected.set(false)
                        isReconnecting.set(true)

                        if(!reconnStatusSent.get()){
                            reconnStatusSent.set(true)

                            // Reinitializing this because we want to get missed messages
                            startZonedDateTime = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.of("UTC"))

                            emitSdk(RECONNECT, RECONNECTING)
                        }
                    }
                    else -> {}
                }
            }

        // Use actual routable host instead of 0.0.0.0
        for (port in 4221..4223) {
            val host = if (staging) "nats://staging.relay-x.io:$port" else "tls://api.relay-x.io:$port"
            builder.server(host)
        }

        try {
            natsConnection = Nats.connect(builder.build()).also {
                jetStream = it.jetStream()
                clientId = it.serverInfo.clientId.toString()
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

        if (reservedTopics.contains(topic)) throw IllegalArgumentException("Cannot publish to a reserved SDK topic: $topic")

        val finalTopic = finalTopic(topic)
        val sendMessage = MessageInfo(client_id = clientId, id = UUID.randomUUID().toString(), room = topic, message = message, start = Instant.now().toEpochMilli())

        if (!::mapper.isInitialized) {
            mapper = ObjectMapper(MessagePackFactory()).registerKotlinModule()
        }

        val packed: ByteArray = mapper.writeValueAsBytes(sendMessage)
        val packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
        packer.writePayload(packed)
        packer.close()

        if (isConnected.get()) {
            val ack = jetStream?.publish(NatsMessage.builder().subject(finalTopic).data(packer.toByteArray()).build())

            return@withContext ack?.error == null;
        } else {
            offlineMessages.add(mutableMapOf("topic" to topic, "message" to message, "resent" to false))
            false
        }
    }

    suspend fun on(topic: String, listener: (JsonObject) -> Unit) : Boolean = withContext(Dispatchers.IO) {
        validateTopic(topic)

        if(listeners.containsKey(topic)){
            return@withContext false
        }

        listeners[topic] = listener

        if(!reservedTopics.contains(topic)){
            subscribedTopics.add(topic)

            if(isConnected.get()){
                startConsumer(topic)
            }
        }

        return@withContext true
    }

    fun off(topic: String) : Boolean {
        validateTopic(topic)
        listeners.remove(topic)
        subscribedTopics.remove(topic)

        consumerJobs.remove(topic)?.cancel()

        if(!reservedTopics.contains(topic)){
            return deleteConsumer(topic)
        }else{
            return true
        }
    }

    suspend fun history(topic: String, start: Long, end: Long?): List<Any> = withContext(Dispatchers.IO) {
        validateTopic(topic)
        requireNotNull(start) { "Start date cannot be null" }

        if (end != null && end <= start) throw IllegalArgumentException("End date <= start date")

        if (!isConnected.get()) return@withContext emptyList()

        val finalTopic = finalTopic(topic)
        val result = mutableListOf<Any>()
        val consumerName = "android_${UUID.randomUUID()}_history_consumer"
        val zonedDateTime = Instant.ofEpochMilli(start).atZone(ZoneId.of("UTC"))

        val config = ConsumerConfiguration.builder()
            .name(consumerName)
            .filterSubject(finalTopic)
            .startTime(zonedDateTime)
            .ackPolicy(AckPolicy.Explicit)
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

                        result.add(JsonObject().apply {
                            addProperty("id", unpacked.id)
                            addProperty("topic", unpacked.room)
                            add("message", Gson().toJsonTree(unpacked.message))
                            addProperty("timestamp", unpacked.start)
                        })
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

    suspend fun close() = withContext(Dispatchers.IO) {
        try {
            isManuallyDisconnected.set(true)
            isReconnecting.set(false)
            connectCalled.set(false)

            latencyTimerJob?.cancel()
            latencyTimerJob = null

            deleteAllConsumers();

            consumerJobs.values.forEach { it.cancel() }
            consumerJobs.clear()

            offlineMessages.clear()

            natsConnection?.close()
            isConnected.set(false)
        } catch (e: Exception) {
            logCatDebug("Error on close: ${e}")
        }
    }

    // ---- Internal Methods -----

    private fun getNamespace(): JsonObject? {
        return try {
            val originalJson = JsonWriter.toJsonBytes(getRequestBody())
            val responseMsg = natsConnection?.request("accounts.user.get_namespace", originalJson, Duration.ofSeconds(5))

            val responseStr = String(responseMsg?.data ?: byteArrayOf(), StandardCharsets.UTF_8)
            logCatDebug(responseStr)

            val responseJson = JsonParser.parseString(responseStr).asJsonObject

            logCatDebug(responseJson)

            if (responseJson.get("status").asString == "NAMESPACE_RETRIEVE_SUCCESS") {
                responseJson.get("data").asJsonObject
            } else null
        } catch (e: Exception) {
            logCatDebug("Namespace fetch failed: ${e.message}")
            null
        }
    }

    // Consumers
    private fun startConsumer(topic: String) {
        val job = ioScope.launch {
            val finalTopic = finalTopic(topic)

            var startTime: ZonedDateTime?

            if(startZonedDateTime != null){
                startTime = startZonedDateTime
            }else{
                startTime = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.of("UTC"))
            }

            val consumerConfig = ConsumerConfiguration.builder()
                .name("android_${UUID.randomUUID()}_consumer")
                .filterSubject(finalTopic)
                .ackPolicy(AckPolicy.Explicit)
                .startTime(startTime)
                .deliverPolicy(DeliverPolicy.ByStartTime)
                .replayPolicy(ReplayPolicy.Instant)
                .build()

            val streamContext = jetStream?.getStreamContext(getStreamName())
            val consumer = streamContext?.createOrUpdateConsumer(consumerConfig);

            consumer?.consume{ msg ->
                val msgTopic = stripTopicHash(msg.subject)

                try {
                    val receivedTime = Instant.now().toEpochMilli()
                    msg.inProgress();

                    val unpacked = mapper.readValue(msg.data, MessageInfo::class.java)
                    val msgClientId = unpacked.client_id

                    logCatDebug("Sent => ${unpacked.start}")
                    logCatDebug("Latency => ${receivedTime - unpacked.start}")

                    logCatDebug(unpacked.toString())

                    if (msgClientId != clientId) {
                        val match = topicPatternMatcher(topic, msgTopic)
                        logCatDebug("$topic || $msgTopic => $match")

                        if(match){
                            callbackScope.launch {
                                listeners[topic]?.invoke(JsonObject().apply {
                                    addProperty("id", unpacked.id)
                                    addProperty("topic", msgTopic)
                                    add("data", Gson().toJsonTree(unpacked.message))
                                })
                            }
                        }

                        logLatency(unpacked.start, receivedTime)
                    }

                    msg.ack()
                } catch (e: Exception) {
                    msg.nakWithDelay(Duration.ofSeconds(5))
                    logCatDebug("Consumer error [$topic]: ${e.message}")
                }
            }

            consumerMap[topic] = consumer!!
        }

        consumerJobs[topic] = job
    }

    private fun deleteConsumer(topic: String) : Boolean{
        val consumer = consumerMap[topic]

        if(consumer != null){
            val consumerName = consumer.consumerName;

            logCatDebug("Consumer to delete => $consumerName")
            try{
                val streamContext = jetStream?.getStreamContext(getStreamName())
                streamContext?.deleteConsumer(consumerName)

                consumerMap.remove(topic)

                logCatDebug("Consumer deleted => ${consumerName}")

                return true
            }catch (e: Exception){
                logCatDebug("ERR => " + e.message)

                return false
            }
        }

        return false
    }

    private fun deleteAllConsumers(){
        for(topic in subscribedTopics){
            deleteConsumer(topic)
        }
    }

    private fun subscribeToTopics() {
        logCatDebug("Subscribing to topics...")
        for(topic in subscribedTopics){
            logCatDebug("Subscribing to $topic")
            startConsumer(topic)
            logCatDebug("Subscribed to $topic")
        }
    }

    private fun resubscribeToTopics(){
        deleteAllConsumers()
        subscribeToTopics()

        startZonedDateTime = null
    }

    // ---- Latency Logging ----
    private fun logLatency(sentTime: Long, receivedTime: Long) {
        val latency = receivedTime - sentTime

        if(latency <= 0){
            return
        }

        latencyHistory.add(JsonObject().apply {
            addProperty("latency", latency)
            addProperty("timestamp", receivedTime)
        })
        flushLatencyLog()
    }

    private fun flushLatencyLog(force: Boolean = false) {
        val now = System.currentTimeMillis()
        if (latencyHistory.isEmpty()) return

        val shouldFlush = latencyHistory.size >= 100 || force || (now - lastLatencyFlushTime) >= 30_000
        if (!shouldFlush) return

        val timezone = TimeZone.getDefault().id;
        val latencies = latencyHistory.toList();

        val modelData = getLatencyBody(timezone, latencies)

        val gson = Gson()

        // We're doing this because we're converting from JSONObject to JsonObject
        val payloadElement = JsonParser.parseString(modelData.payload.toString())

        val requestBody = JsonObject()
        requestBody.addProperty("api_key", modelData.api_key)
        requestBody.add("payload", payloadElement)

        val payload = gson.toJson(requestBody).toByteArray(StandardCharsets.UTF_8)

        val responseMsg = natsConnection?.request("accounts.user.log_latency", payload, Duration.ofSeconds(5))
        val responseStr = String(responseMsg?.data ?: byteArrayOf(), StandardCharsets.UTF_8)
        logCatDebug(responseStr)

        logCatDebug("Published latency log with ${latencyHistory.size} entries")

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

        emitSdk(MESSAGE_RESEND, mapOf("data" to result))
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

    private fun validateTopic(topic: String) {
        val topicNotNull = !topic.isBlank()

        val topicRegex = Regex("^(?!.*\\\$)(?:[A-Za-z0-9_*~-]+(?:\\.[A-Za-z0-9_*~-]+)*(?:\\.>)?|>)\$")

        val spaceStarCheck = !topic.contains(" ") && topicRegex.matches(topic)

        require(spaceStarCheck && topicNotNull) { "Invalid topic" }
    }

    fun isTopicValid(topic: String) : Boolean {
        val topicNotNull = !topic.isBlank()

        val topicRegex = Regex("^(?!.*\\\$)(?:[A-Za-z0-9_*~-]+(?:\\.[A-Za-z0-9_*~-]+)*(?:\\.>)?|>)\$")

        val spaceStarCheck = !topic.contains(" ") && topicRegex.matches(topic) && !reservedTopics.contains(topic)

        return spaceStarCheck && topicNotNull
    }

    private fun validateMessage(msg: Any) {
        require(msg is String || msg is Number || msg is Map<*, *>) { "Message must be string, number or Map<String, String | Number | Map>" }
    }

    fun isMessageValid(msg: Any) {
        require(msg is String || msg is Number || msg is Map<*, *>) { "Message must be string, number or Map<String, String | Number | Map>" }
    }

    private fun validateEmptyMessage(msg: Any) {
        require(msg != null) { "Message must not be null or empty" }
    }

    private fun finalTopic(topic: String): String = "$hash.$topic"

    private fun emitSdk(topic: String, message: Any) {
        callbackScope.launch {
            listeners[topic]?.invoke(JsonObject().apply {
                add("data", Gson().toJsonTree(message))
            })
        }
    }

    private fun getRequestBody(): RequestBody {
        val requestBody = RequestBody()
        requestBody.api_key = apiKey
        return requestBody
    }

    private fun getLatencyBody(timeZone: String, history: List<JsonObject>) : LatencyBody {
        val requestBody = LatencyBody()
        requestBody.api_key = this.apiKey
        requestBody.payload = JsonObject().apply {
            addProperty("timezone", timeZone)
            add("history", Gson().toJsonTree(history))
        }
        return requestBody
    }

    private fun logCatDebug(message: Any) {
        if (debug) Log.i("RealtimeSDK", "$message")
    }

    fun checkIsConnected(): Boolean {
        return isConnected.get()
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

    fun getNamespaceTest(): String? {
        logCatDebug("IS CONNECTED => ${checkIsConnected()}")
        return namespace;
    }

    fun getHashTest(): String? {
        return hash;
    }

    fun getStreamName(): String = "${namespace}_stream"

    fun stripTopicHash(topic: String): String = topic.replace("${hash}.", "")
}
