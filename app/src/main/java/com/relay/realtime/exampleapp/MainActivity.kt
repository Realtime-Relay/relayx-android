package com.relay.realtime.exampleapp

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.os.Bundle
import android.os.IBinder
import android.view.View
import android.widget.Button
import android.widget.EditText
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.google.gson.JsonObject
import com.relay.realtime.R

class MainActivity : AppCompatActivity(), RealtimeService.Listener {

    private var bound = false
    private var svc: RealtimeService? = null

    // ── UI ────────────────────────────────────────────────────────────────
    private lateinit var topicInput: EditText
    private lateinit var connectBtn: Button
    private lateinit var statusTxt: TextView
    private lateinit var recycler: RecyclerView
    private lateinit var msgInput: EditText
    private lateinit var sendBtn: Button

    private val messages = mutableListOf<MessageItem>()
    private val adapter = MessageAdapter(messages)

    // ── Service connection ───────────────────────────────────────────────
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

    // ── Lifecycle ─────────────────────────────────────────────────────────
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        bindViews()
        hookClicks()
        updateUi(connected = false)
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

    // ── Helpers ───────────────────────────────────────────────────────────
    private fun bindViews() {
        topicInput = findViewById(R.id.topicInput)
        connectBtn = findViewById(R.id.connectBtn)
        statusTxt = findViewById(R.id.statusTxt)
        recycler = findViewById(R.id.chatRecycler)
        msgInput = findViewById(R.id.messageInput)
        sendBtn = findViewById(R.id.sendBtn)

        recycler.layoutManager = LinearLayoutManager(this).apply { stackFromEnd = true }
        recycler.adapter = adapter
    }

    private fun hookClicks() {
        connectBtn.setOnClickListener {
            if (statusTxt.text == "Connected") {
                svc?.disconnect()
            } else {
                svc?.connect()
            }
        }

        sendBtn.setOnClickListener {
            val topic = topicInput.text.toString().trim()
            val msg = msgInput.text.toString().trim()
            if (topic.isNotEmpty() && msg.isNotEmpty()) {
                svc?.publish(topic, msg) { ok ->
                    if (ok) addChatLine("me", msg)
                }
                msgInput.text.clear()
            }
        }
    }

    private fun addChatLine(from: String, body: String) {
        runOnUiThread {
            messages += MessageItem(from, body)
            adapter.notifyItemInserted(messages.lastIndex)
            recycler.scrollToPosition(messages.lastIndex)
        }
    }

    private fun updateUi(connected: Boolean) {
        runOnUiThread {
            statusTxt.text = if (connected) "Connected" else "Disconnected"
            statusTxt.setTextColor(resources.getColor(if (connected) android.R.color.holo_green_dark else android.R.color.holo_red_dark, theme))
            msgInput.isEnabled = connected
            sendBtn.isEnabled = connected
            recycler.visibility = if (connected) View.VISIBLE else View.INVISIBLE
        }
    }

    // ── Listener callbacks from Service ───────────────────────────────────
    override fun onSdkEvent(event: String, payload: JsonObject) {
        when (event) {
            "CONNECTED"   -> {
                updateUi(true)
                val t = topicInput.text.toString().trim()
                if (t.isNotEmpty()) svc?.subscribe(t)
            }
            "DISCONNECTED" -> updateUi(false)
        }
    }

    override fun onMessage(topic: String, payload: JsonObject) {
        val msg = payload.get("message").asString
        addChatLine(topic, msg)
    }
}