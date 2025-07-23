package com.relay.realtime.exampleapp

import android.app.Service
import android.content.Intent
import android.os.Binder
import android.os.IBinder
import android.util.Log
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.relay.realtime.realtimeSDK.Realtime
import com.relay.realtime.realtimeSDK.Utils
import kotlinx.coroutines.*

class RealtimeService : Service(), CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.IO) {

    /** Activity implements this so Service can push SDK events / messages up. */
    interface Listener {
        fun onSdkEvent(event: String, payload: JsonObject)
        fun onMessage(topic: String, payload: JsonObject)
    }

    private val binder = LocalBinder()
    private lateinit var realtime: Realtime
    private var listener: Listener? = null

    inner class LocalBinder : Binder() {
        fun getService(): RealtimeService = this@RealtimeService
    }

    override fun onCreate() {
        super.onCreate()

        // Initialise SDK once for the whole app lifetime
        realtime = Realtime(this, Utils.API_KEY, Utils.SECRET_KEY).apply {
            init(staging = false, opts = mapOf("debug" to true))
        }

        // Forward core SDK events to whoever is listening
        launch {
            realtime.on(Realtime.CONNECTED) { listener?.onSdkEvent(Realtime.CONNECTED, it) }
            realtime.on(Realtime.DISCONNECTED) { listener?.onSdkEvent(Realtime.DISCONNECTED, it) }
            realtime.on(Realtime.RECONNECT) { listener?.onSdkEvent(Realtime.RECONNECT, it) }
            realtime.on(Realtime.MESSAGE_RESEND) { listener?.onSdkEvent(Realtime.MESSAGE_RESEND, it) }

            realtime.on("hello.>") { payload -> listener?.onMessage("hello.>", payload) }
        }
    }

    override fun onBind(intent: Intent?): IBinder = binder

    // ── Public helpers — thin wrappers that launch on the Service’s IO scope ──
    fun setListener(l: Listener?) { listener = l }

    fun connect()  = launch { realtime.connect() }
    fun disconnect() = launch { realtime.close() }

    fun subscribe(topic: String) = launch {
        realtime.on(topic) { payload -> listener?.onMessage(topic, payload) }
    }

    fun unsubscribe(topic: String) { realtime.off(topic) }

    fun publish(topic: String, message: String, cb: (Boolean) -> Unit = {}) = launch {
        val ok = realtime.publish(topic, message)
        withContext(Dispatchers.Main) { cb(ok) }
    }

    fun history(topic: String, start: Long, end: Long, cb: (String) -> Unit) = launch {
        Log.i("RealtimeSDK", "Calling history...")
        val hist = realtime.history(topic, start, end)
        val json = Gson().toJson(hist)
        withContext(Dispatchers.Main) { cb(json) }
    }

    override fun onDestroy() {
        super.onDestroy()
        cancel()           // kill coroutines
//        realtime.close()
    }
}