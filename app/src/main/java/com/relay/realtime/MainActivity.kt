package com.relay.realtime

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.os.Bundle
import android.os.IBinder
import android.widget.*
import androidx.appcompat.app.AppCompatActivity
import com.google.gson.JsonObject
import org.json.JSONObject

class MainActivity : AppCompatActivity(), RealtimeService.Listener {

    private var bound = false
    private var svc: RealtimeService? = null

    // UI refs
    private lateinit var connectBtn: Button
    private lateinit var disconnectBtn: Button
    private lateinit var subscribeBtn: Button
    private lateinit var unsubscribeBtn: Button
    private lateinit var publishBtn: Button
    private lateinit var historyBtn: Button
    private lateinit var topicInput: EditText
    private lateinit var messageInput: EditText
    private lateinit var logView: TextView

    // Service connection //////////////////////////////////////////////////////
    private val connection = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName?, binder: IBinder?) {
            svc = (binder as RealtimeService.LocalBinder).getService().apply {
                setListener(this@MainActivity)
            }
            bound = true
        }

        override fun onServiceDisconnected(name: ComponentName?) {
            bound = false
            svc = null
        }
    }

    // Lifecycle ////////////////////////////////////////////////////////////////
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        bindViews();
        hookClicks()
    }

    override fun onStart() {
        super.onStart()
        Intent(this, RealtimeService::class.java).also {
            bindService(it, connection, Context.BIND_AUTO_CREATE)
        }
    }

    override fun onStop() {
        super.onStop()
        if (bound) {
            svc?.setListener(null)
            unbindService(connection)
            bound = false
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        // Service lives on; activity merely unbinds.
    }

    // UI helpers ///////////////////////////////////////////////////////////////
    private fun bindViews() {
        connectBtn = findViewById(R.id.connectBtn)
        disconnectBtn = findViewById(R.id.disconnectBtn)
        subscribeBtn = findViewById(R.id.subscribeBtn)
        unsubscribeBtn = findViewById(R.id.unsubscribeBtn)
        publishBtn = findViewById(R.id.publishBtn)
        historyBtn = findViewById(R.id.historyBtn)
        topicInput = findViewById(R.id.topicInput)
        messageInput = findViewById(R.id.messageInput)
        logView = findViewById(R.id.messageLog)
    }

    private fun hookClicks() {
        connectBtn.setOnClickListener { svc?.connect() }
        disconnectBtn.setOnClickListener { svc?.disconnect() }

        subscribeBtn.setOnClickListener {
            val topic = topicInput.text.toString().trim()
            if (topic.isNotEmpty()) svc?.subscribe(topic)
        }

        unsubscribeBtn.setOnClickListener {
            val topic = topicInput.text.toString().trim()
            if (topic.isNotEmpty()) svc?.unsubscribe(topic)
        }

        publishBtn.setOnClickListener {
            val topic = topicInput.text.toString().trim()
            val msg = messageInput.text.toString().trim()
            if (topic.isNotEmpty() && msg.isNotEmpty()) {
                svc?.publish(topic, msg) { ok -> append("Publish → $ok") }
            }
        }

        historyBtn.setOnClickListener {
            val topic = topicInput.text.toString().trim()
            if (topic.isNotEmpty()) {
                val since = System.currentTimeMillis() - 5 * 60 * 60 * 1_000
                svc?.history(topic, since, System.currentTimeMillis()) { json ->
                    append("History for $topic:\n$json")
                }
            }
        }
    }

    private fun append(text: String) {
        runOnUiThread { logView.append("\n\n➤ $text") }
    }

    override fun onSdkEvent(event: String, payload: JsonObject) {
        append("SDK • $event → $payload")
    }

    override fun onMessage(topic: String, payload: JsonObject) {
        append("$topic → $payload")
    }
}