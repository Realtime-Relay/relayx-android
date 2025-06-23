package com.relay.realtime

import android.os.Handler
import android.os.Looper
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONObject

class RelayConnectionManager(private val auth: RelayAuth) {

    private var webSocket: WebSocket? = null
    private val client = OkHttpClient()
    private var isManualDisconnect = false

    fun connect() {
        isManualDisconnect = false

        val request = Request.Builder()
            .url("wss://api.relay-x.io") // ✅ Updated to your backend
            .addHeader("Authorization", "Bearer ${auth.jwt}")
            .addHeader("X-Secret", auth.secretKey)
            .build()

        client.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(ws: WebSocket, response: Response) {
                println("✅ Connected to Relay backend")
                webSocket = ws
            }

            override fun onMessage(ws: WebSocket, text: String) {
                val json = JSONObject(text)
                RelaySDK.onMessage(json)
            }

            override fun onFailure(ws: WebSocket, t: Throwable, response: Response?) {
                println("❌ Connection failed: ${t.message}")
                if (!isManualDisconnect) reconnect()
            }

            override fun onClosed(ws: WebSocket, code: Int, reason: String) {
                println("🔌 Disconnected: $reason")
            }
        })
    }

    fun send(message: String) {
        webSocket?.send(message)
    }

    fun disconnect(manual: Boolean) {
        isManualDisconnect = manual
        webSocket?.close(1000, "Manual disconnect")
    }

    fun reconnect() {
        Handler(Looper.getMainLooper()).postDelayed({ connect() }, 3000)
    }
}
