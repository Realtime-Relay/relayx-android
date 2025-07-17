package com.relay.realtime.realtimeSDK

import android.content.Context
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.json.JSONObject
import org.junit.*
import org.junit.Assert.*
import org.junit.runner.RunWith
import org.mockito.Mock
import org.mockito.junit.MockitoJUnitRunner
import java.util.*
import java.util.concurrent.ConcurrentHashMap

@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(MockitoJUnitRunner::class)
class RealtimeTest {

    private lateinit var realtime: Realtime

    @Mock
    private lateinit var context: Context

    private val apiKey = Utils.API_KEY
    private val secretKey = Utils.SECRET_KEY
    private val staging = false

    @Before
    fun setup() = runTest {
        realtime = Realtime(
            context = context,
            apiKey = apiKey,
            secretKey = secretKey
        )
        realtime.init(staging = staging, opts = mapOf("debug" to true))
//        realtime.connect()
    }

    @After
    fun teardown() {
        realtime.close()
    }

    @Test
    fun `test throws error when no config is passed`() {
        val exception = assertThrows(IllegalArgumentException::class.java) {
            Realtime(context = context, apiKey = "", secretKey = "")
        }
        assertEquals("apiKey must not be empty", exception.message)
    }

    @Test
    fun `test throws error when only apiKey is passed`() {
        val exception = assertThrows(IllegalArgumentException::class.java) {
            Realtime(context = context, apiKey = "KEY", secretKey = "")
        }
        assertEquals("secretKey must not be empty", exception.message)
    }

    @Test
    fun `test throws error when only secretKey is passed`() {
        val exception = assertThrows(IllegalArgumentException::class.java) {
            Realtime(context = context, apiKey = "", secretKey = "SECRET")
        }
        assertEquals("apiKey must not be empty", exception.message)
    }

    @Test
    fun `test throws error when null is passed`() {
        assertThrows(NullPointerException::class.java) {
            Realtime(context = context, apiKey = null!!, secretKey = null!!)
        }
    }

    @Test
    fun `test init function with multiple configurations`() = runBlocking {
        val realtime = Realtime(context, apiKey, secretKey)

        // init(true)
        realtime.init(true, mapOf())
        assertTrue(realtime.getStaging())
        assertEquals(emptyMap<String, Any>(), realtime.getOpts())

        // init({ debug: true, max_retries: 2 })
//        realtime.init(mapOf("debug" to true, "max_retries" to 2))
//        assertFalse(realtime.getStaging())
//        assertEquals(mapOf("debug" to true, "max_retries" to 2), realtime.getOpts())
//        assertEquals(true, realtime.getOpts()?["debug"])
//        assertEquals(2, realtime.getOpts()["max_retries"])
//
//        // init(true, { debug: false, max_retries: 2 })
//        realtime.init(true, mapOf("debug" to false, "max_retries" to 2))
//        assertTrue(realtime.getStaging())
//        assertEquals(mapOf("debug" to false, "max_retries" to 2), realtime.getOpts())
//        assertEquals(false, realtime.getOpts()["debug"])
//        assertEquals(2, realtime.getOpts()["max_retries"])
//
//        // init(false)
//        realtime.init(false)
//        assertFalse(realtime.getStaging())
//        assertEquals(emptyMap<String, Any>(), realtime.getOpts())
//        assertNull(realtime.getOpts()["debug"])
//        assertNull(realtime.getOpts()["max_retries"])
//
//        // init()
//        realtime.init()
//        assertFalse(realtime.getStaging())
//        assertEquals(emptyMap<String, Any>(), realtime.getOpts())
//        assertNull(realtime.getOpts()["debug"])
//        assertNull(realtime.getOpts()["max_retries"])
    }

    @Test
    fun `init sets flags correctly`() {
        val r = Realtime(context, apiKey, secretKey)
        r.init(staging, mapOf("debug" to true))
        assertTrue(r.checkIsConnected().not()) // Should not be connected yet
    }

    @Test
    fun `publish works and stores offline message when not connected`() = runTest {
        val r = Realtime(context, apiKey, secretKey)
        r.init(staging, mapOf("debug" to true))

        println("Offline: " + r.checkIsConnected())
        val offlineMessages = Collections.synchronizedList(mutableListOf<MutableMap<String, Any?>>())
        offlineMessages.add(mutableMapOf("topic" to "offline.topic", "message" to "offline", "resent" to false))


        r.offlineMessage(offlineMessages) // should not crash
    }

    @Test
    fun `on and off functions properly`() = runTest {
        val topic = "test-topic"
        var called = false

        realtime.on(topic) {
            called = true
        }

        assertTrue(realtime.listenersList().containsKey(topic))

        realtime.off(topic)
        assertFalse(realtime.listenersList().containsKey(topic))
    }

    @Test
    fun `off returns false for unknown topic`() = runTest {
        val result = realtime.off("unknown-topic")
        assertFalse(result)
    }

    @Test
    fun `publish rejects reserved topics`() = runTest {
        val reserved = listOf("CONNECTED", "RECONNECT", "DISCONNECTED", "MESSAGE_RESEND")

        for (topic in reserved) {
            assertThrows(IllegalArgumentException::class.java) {
                runBlocking {
                    realtime.publish(topic, mapOf("msg" to "bad"))
                }
            }
        }
    }

    fun assertThrowsOnPublish(topic: String, message: Any) {
        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                realtime.publish(topic, message)
            }
        }
    }

    @Test
    fun `publish throws on invalid message`() {
        assertThrowsOnPublish("valid", "")
        assertThrowsOnPublish("valid", 1.2)
    }


    @Test
    fun `publish validates topic and message`() = runTest {
        val invalidTopics = listOf("", " ", "*invalid*", "in valid")

        for (topic in invalidTopics) {
            assertThrows(IllegalArgumentException::class.java) {
                runBlocking {
                    realtime.publish(topic, "data")
                }
            }
        }

        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                realtime.publish("valid", listOf(1, 2, 3)) // Invalid message type
            }
        }

        val resultEmpty = realtime.publish("valid", "")
        assertTrue("Invalid message should be published", resultEmpty)

        val result = realtime.publish("valid", mapOf("key" to "value"))
        assertTrue("Valid message should be published", result)

    }

    @Test
    fun `history rejects invalid arguments`() = runTest {
        val now = System.currentTimeMillis()

        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                realtime.history("", now, null)
            }
        }

        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                realtime.history("topic", now, now - 1000)
            }
        }

        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                realtime.history("topic", now, null)
            }
        }
    }

    @Test
    fun `flush latency log executes correctly`() = runTest {
        realtime.flushLatencyLogPublic(force = true)
    }

    @Test
    fun `resend offline messages triggers MESSAGE_RESEND`() = runTest {
        var triggered = false

        val field = Realtime::class.java.getDeclaredField("sdkListeners")
        field.isAccessible = true
        val listeners = field.get(realtime) as ConcurrentHashMap<String, (Any) -> Unit>
        listeners["MESSAGE_RESEND"] = { triggered = true }

        runBlocking {
            val offlineMessages = Collections.synchronizedList(mutableListOf<MutableMap<String, Any?>>())
            offlineMessages.add(mutableMapOf("topic" to "offline.topic", "message" to "offline", "resent" to false))


            realtime.offlineMessage(offlineMessages)
        }

        assertTrue(triggered)
    }

    @Test
    fun `multiple on calls do not re-subscribe`() = runTest {
        val topic = "dedupe-topic"
        val listener: (JSONObject) -> Unit = {}

        realtime.on(topic, listener)
        realtime.on(topic, listener)
        realtime.on(topic, listener)

        assertEquals(1, realtime.listenersList().count { it.key == topic })
    }
}
