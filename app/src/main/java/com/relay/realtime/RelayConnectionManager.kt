package com.relay.realtime

import android.os.Handler
import android.os.Looper
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener

class RelayConnectionManager(private val auth: RelayAuth) {

    private var webSocket: WebSocket? = null
    private var client = OkHttpClient()
    private var isManualDisconnect = false

    fun connect() {
        isManualDisconnect = false
        val request = Request.Builder()
            .url("wss://api.relay-x.io") // your backend URL
            .addHeader("Authorization", "Bearer ${auth.jwt}")
            .addHeader("X-Secret", auth.secretKey)
            .build()

        webSocket = client.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(ws: WebSocket, response: Response) {
                println("Connected")
            }

            override fun onFailure(ws: WebSocket, t: Throwable, response: Response?) {
                println("Connection failed: ${t.message}")
                if (!isManualDisconnect) {
                    reconnect()
                }
            }

            override fun onClosed(ws: WebSocket, code: Int, reason: String) {
                println("Closed: $reason")
            }
        })
    }

    fun disconnect(manual: Boolean = true) {
        isManualDisconnect = manual
        webSocket?.close(1000, "Client disconnect")
    }

    fun reconnect() {
        disconnect(false)
        Handler(Looper.getMainLooper()).postDelayed({
            connect()
        }, 3000)
    }

    fun send(message: String) {
        webSocket?.send(message)
    }

    fun getWebSocket(): WebSocket? = webSocket
}
