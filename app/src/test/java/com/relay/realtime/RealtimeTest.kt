package com.relay.realtime.realtimeSDK

import android.content.Context
import com.fasterxml.jackson.databind.ObjectMapper
import io.nats.client.*
import io.nats.client.api.ServerInfo
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.*
import org.json.JSONObject
import org.junit.*
import org.junit.runner.RunWith
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.MockitoAnnotations
import org.mockito.junit.MockitoJUnitRunner
import java.io.File
import java.lang.reflect.InvocationTargetException
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(MockitoJUnitRunner::class)
class RealtimeTest {

    private lateinit var realtime: Realtime

    @Mock
    private lateinit var context: Context

//    private val apiKey = "test-api-key"
//    private val secretKey = "test-secret-key"

    private val apiKey = Utils.API_KEY
    private val secretKey = Utils.SECRET_KEY

    @Before
    fun setUp() {
        Dispatchers.setMain(UnconfinedTestDispatcher())
        realtime = Realtime(context, apiKey, secretKey)
    }

    @After
    fun tearDown() {
        Dispatchers.resetMain()
    }

    @Test(expected = IllegalArgumentException::class)
    fun `init throws if apiKey is blank`() {
        Realtime(context, "", secretKey)
    }

    @Test(expected = IllegalArgumentException::class)
    fun `init throws if secretKey is blank`() {
        Realtime(context, apiKey, "")
    }

    @Test
    fun `init sets staging and debug`() {
        val opts = mapOf("debug" to true)
        realtime.init(staging = true, opts = opts)

        val stagingField = Realtime::class.java.getDeclaredField("staging")
        stagingField.isAccessible = true
        val debugField = Realtime::class.java.getDeclaredField("debug")
        debugField.isAccessible = true

        assertTrue(stagingField.get(realtime) as Boolean)
        assertTrue(debugField.get(realtime) as Boolean)
    }

    @Test(expected = IllegalArgumentException::class)
    fun `init throws if opts is null`() {
        realtime.init(staging = false, opts = null)
    }

    @Test
    fun `init is successful`() {
        realtime.init(staging = false, opts = mapOf("debug" to true))
    }

    @Test
    fun testConnectSuccess() = runBlocking {
        // Ideally mock Nats.connect and response of request to getNamespace
        try {
            realtime.connect()
            assertTrue(realtime.checkIsConnected())
        } catch (e: Exception) {
            fail("Connect should not throw exception: ${e.message}")
        }
    }

    @Test
    fun testPublishOfflineAndResend() = runBlocking {
        @Test
        fun testPublishOfflineAndResend() = runBlocking {
            val message = mapOf("key" to "value")
            val published = realtime.publish("test.topic", message)
            assertFalse(published)

            realtime.connect() // mocked
            realtime.offlineMessage()

            // Optional: verify internal state or hook into MESSAGE_RESEND via sdkListeners
        }


        // Ideally verify internal state or hook into MESSAGE_RESEND
    }

    @Test
    fun testOnTopicRegistersListener() {
        val called = AtomicBoolean(false)
        realtime.on("chat.test") { msg ->
            called.set(true)
            assertNotNull(msg)
        }
        assertTrue(realtime.listenersList().containsKey("chat.test"))
    }

    @Test
    fun testOffRemovesListenerAndConsumer() {
        realtime.on("chat.remove") { /* no-op */ }
        val result = realtime.off("chat.remove")
        assertTrue(result)
        assertFalse(realtime.listenersList().containsKey("chat.remove"))
    }

    @Test
    fun testHistoryReturnsEmptyWhenDisconnected() = runBlocking {
        val result = realtime.history("chat.history", start = System.currentTimeMillis() - 1000, end = null)
        assertTrue(result.isEmpty())
    }

    @Test
    fun testFlushLatencyLogForcesSend() {
        realtime.flushLatencyLogPublic(force = true)
        // Can’t verify directly unless you expose/log something.
    }

    @Test
    fun testInvalidTopicThrowsException() {
        val method = Realtime::class.java.getDeclaredMethod("validateTopic", String::class.java)
        method.isAccessible = true

        try {
            method.invoke(realtime, "invalid topic with space")
            fail("Expected IllegalArgumentException was not thrown")
        } catch (e: InvocationTargetException) {
            assertTrue(e.cause is IllegalArgumentException)
            assertEquals("Invalid topic", e.cause?.message)
        }
    }


    @Test
    fun testCloseReleasesResources() {
        realtime.close()
        assertFalse(realtime.checkIsConnected())
    }

    @Test
    fun testLogLatencyAddsEntry() {
        val start = System.currentTimeMillis() - 100
        val end = System.currentTimeMillis()
        val method = Realtime::class.java.getDeclaredMethod("logLatency", Long::class.java, Long::class.java)
        method.isAccessible = true
        method.invoke(realtime, start, end)

        val latencyField = Realtime::class.java.getDeclaredField("latencyHistory")
        latencyField.isAccessible = true
        val list = latencyField.get(realtime) as List<*>
        assertTrue(list.isNotEmpty())
    }


}
