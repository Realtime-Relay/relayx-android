package com.relay.realtime

import android.os.Bundle
import android.widget.*
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import com.google.gson.Gson
import com.relay.realtime.realtimeSDK.Realtime
import com.relay.realtime.realtimeSDK.Utils
import com.relay.realtime.realtimeSDK.Utils.createNatsCredsFile
import kotlinx.coroutines.*


class MainActivity : AppCompatActivity() {

    private lateinit var realtime: Realtime

    private lateinit var connectBtn: Button
    private lateinit var disconnectBtn: Button
    private lateinit var subscribeBtn: Button
    private lateinit var unsubscribeBtn: Button
    private lateinit var publishBtn: Button
    private lateinit var historyBtn: Button
    private lateinit var topicInput: EditText
    private lateinit var messageInput: EditText
    private lateinit var messageLog: TextView

    private val scope = CoroutineScope(Dispatchers.IO)
    private val logTag = "RealtimeUI"

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main) // XML provided below

        // Bind views
        connectBtn = findViewById(R.id.connectBtn)
        disconnectBtn = findViewById(R.id.disconnectBtn)
        subscribeBtn = findViewById(R.id.subscribeBtn)
        unsubscribeBtn = findViewById(R.id.unsubscribeBtn)
        publishBtn = findViewById(R.id.publishBtn)
        historyBtn = findViewById(R.id.historyBtn)
        topicInput = findViewById(R.id.topicInput)
        messageInput = findViewById(R.id.messageInput)
        messageLog = findViewById(R.id.messageLog)

        // Init SDK
        realtime = Realtime(this, Utils.API_KEY, Utils.SECRET_KEY)
        realtime.init(staging = false, opts = mapOf("debug" to true))

        // Event handlers
        connectBtn.setOnClickListener {
            scope.launch {
                realtime.connect()
                appendLog("Connected to Relay")
            }
        }

        disconnectBtn.setOnClickListener {
            realtime.close()
            appendLog("Disconnected from Relay")
        }

        subscribeBtn.setOnClickListener {
            val topic = topicInput.text.toString().trim()
            if (topic.isNotEmpty()) {
                lifecycleScope.launch {
                    realtime.on(topic) { msg ->
                        appendLog("Message received: $msg")
                    }
                    appendLog("Subscribed to $topic")
                }
            }
        }

        unsubscribeBtn.setOnClickListener {
            val topic = topicInput.text.toString().trim()
            if (topic.isNotEmpty()) {
                scope.launch {
                    realtime.off(topic)
                    appendLog("Unsubscribed to $topic")
                }
            }
        }

        publishBtn.setOnClickListener {
            val topic = topicInput.text.toString().trim()
            val message = messageInput.text.toString().trim()
            if (topic.isNotEmpty() && message.isNotEmpty()) {
                scope.launch {
                    val success = realtime.publish(topic, message)
                    appendLog("Message published: $success")
                }
            }
        }

        historyBtn.setOnClickListener {
            val topic = topicInput.text.toString().trim()
            if (topic.isNotEmpty()) {
                scope.launch {
                    val fiveHoursAgoInMillis = System.currentTimeMillis() - (5 * 60 * 60 * 1000)

                    val history = realtime.history(
                        topic = topic,
                        start = fiveHoursAgoInMillis,
                        end = System.currentTimeMillis()
                    )

                    val gson = Gson()
                    val jsonString = gson.toJson(history)

                    appendLog("History:\n" + jsonString)
                }
            }
        }

        // SDK event listeners
        CoroutineScope(Dispatchers.IO).launch {
            realtime.on("CONNECTED") { appendLog("SDK: CONNECTED") }
            realtime.on("DISCONNECTED") { appendLog("SDK: DISCONNECTED") }
            realtime.on("RECONNECTED") { appendLog("SDK: RECONNECTED") }
            realtime.on("MESSAGE_RESEND") { appendLog("SDK: MESSAGE_RESEND\n$it") }
        }
    }

    private fun appendLog(msg: String) {
        runOnUiThread {
            messageLog.append("➤ $msg\n\n")
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        scope.cancel()
        realtime.close()
    }

}