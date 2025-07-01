package com.relay.realtime

import android.os.Bundle
import androidx.appcompat.app.AppCompatActivity
import android.util.Log
import android.widget.*
import androidx.lifecycle.lifecycleScope
import com.relay.realtime.realtimeSDK.Realtime
import com.relay.realtime.realtimeSDK.Utils
import com.relay.realtime.realtimeSDK.Utils.createNatsCredsFile
import kotlinx.coroutines.*
import org.json.JSONObject
import java.time.LocalDateTime

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
        val credsFile = createNatsCredsFile(this, Utils.API_KEY, Utils.SECRET_KEY)

        // Init SDK
        realtime = Realtime(this@MainActivity, Utils.API_KEY, Utils.SECRET_KEY)
        realtime.init(staging = false, opts = mapOf("debug" to true))

        // Event handlers
        connectBtn.setOnClickListener {
            scope.launch {
                realtime.connect(credsFile.absolutePath)
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
                scope.launch {
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

                    println("Success: " + success)
                    appendLog("Message published: $success")
                }
            }
        }

        historyBtn.setOnClickListener {
            val topic = topicInput.text.toString().trim()
            if (topic.isNotEmpty()) {
                scope.launch {
                    val history = realtime.history(
                        topic = topic,
                        start = LocalDateTime.now().minusHours(1),
                        end = LocalDateTime.now()
                    )

                    println("History: " + history)
                    appendLog("History:\n" + history.joinToString("\n"))
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