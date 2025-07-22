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
import org.robolectric.shadows.ShadowLog
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.assertFailsWith

@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(MockitoJUnitRunner::class)
class RealtimeTest {

    private lateinit var realtimeEnabled: Realtime

    @Mock
    private lateinit var context: Context

    private val apiKey = Utils.API_KEY
    private val secretKey = Utils.SECRET_KEY
    private val staging = false

    @Before
    fun setup() {
        ShadowLog.stream = System.out

        runBlocking {
            realtimeEnabled = Realtime(
                context = context,
                apiKey = apiKey,
                secretKey = secretKey
            )
            realtimeEnabled.init(staging = staging, opts = mapOf("debug" to true))
            realtimeEnabled.connect()
        }
    }

    @After
    fun teardown() {
        runBlocking {
            realtimeEnabled.close()
        }
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

        realtime.init(false, mapOf())
        assertFalse(realtime.getStaging())
        assertEquals(emptyMap<String, Any>(), realtime.getOpts())

        realtime.init(false, mapOf("debug" to true, "max_retries" to 2))
        assertFalse(realtime.getStaging())
        assertEquals(mapOf("debug" to true, "max_retries" to 2), realtime.getOpts())
        assertEquals(true, realtime.getOpts()?.get("debug"))
        assertEquals(2, realtime.getOpts()?.get("max_retries"))

        // init(true, { debug: false, max_retries: 2 })
        realtime.init(true, mapOf("debug" to false, "max_retries" to 2))
        assertTrue(realtime.getStaging())
        assertEquals(mapOf("debug" to false, "max_retries" to 2), realtime.getOpts())
        assertEquals(false, realtime.getOpts()?.get("debug"))
        assertEquals(2, realtime.getOpts()?.get("max_retries"))
    }

    @Test
    fun `init sets flags correctly`() {
        val r = Realtime(context, apiKey, secretKey)
        r.init(staging, mapOf("debug" to true))
        assertTrue(r.checkIsConnected().not()) // Should not be connected yet
    }

    @Test
    fun `Namespace values test`() {
        assertTrue(realtimeEnabled.getNamespaceTest()?.length!! > 0)
        assertTrue(realtimeEnabled.getHashTest()?.length!! > 0)
    }

//    @Test
//    fun `publish works and stores offline message when not connected`() = runTest {
//        val r = Realtime(context, apiKey, secretKey)
//        r.init(staging, mapOf("debug" to true))
//
//        println("Offline: " + r.checkIsConnected())
//        val offlineMessages = Collections.synchronizedList(mutableListOf<MutableMap<String, Any?>>())
//        offlineMessages.add(mutableMapOf("topic" to "offline.topic", "message" to "offline", "resent" to false))
//
//
//        r.offlineMessage(offlineMessages) // should not crash
//    }
//
//    @Test
//    fun `on and off functions properly`() = runTest {
//        val topic = "test-topic"
//        var called = false
//
//        realtime.on(topic) {
//            called = true
//        }
//
//        assertTrue(realtime.listenersList().containsKey(topic))
//
//        realtime.off(topic)
//        assertFalse(realtime.listenersList().containsKey(topic))
//    }
//
//    @Test
//    fun `off returns false for unknown topic`() = runTest {
//        val result = realtime.off("unknown-topic")
//        assertFalse(result)
//    }
//
//    @Test
//    fun `publish rejects reserved topics`() = runTest {
//        val reserved = listOf("CONNECTED", "RECONNECT", "DISCONNECTED", "MESSAGE_RESEND")
//
//        for (topic in reserved) {
//            assertThrows(IllegalArgumentException::class.java) {
//                runBlocking {
//                    realtime.publish(topic, mapOf("msg" to "bad"))
//                }
//            }
//        }
//    }
//
//    fun assertThrowsOnPublish(topic: String, message: Any) {
//        assertThrows(IllegalArgumentException::class.java) {
//            runBlocking {
//                realtime.publish(topic, message)
//            }
//        }
//    }
//
//    @Test
//    fun `publish throws on invalid message`() {
//        assertThrowsOnPublish("valid", "")
//        assertThrowsOnPublish("valid", 1.2)
//    }
//
//
//    @Test
//    fun `publish validates topic and message`() = runTest {
//        val invalidTopics = listOf("", " ", "*invalid*", "in valid")
//
//        for (topic in invalidTopics) {
//            assertThrows(IllegalArgumentException::class.java) {
//                runBlocking {
//                    realtime.publish(topic, "data")
//                }
//            }
//        }
//
//        assertThrows(IllegalArgumentException::class.java) {
//            runBlocking {
//                realtime.publish("valid", listOf(1, 2, 3)) // Invalid message type
//            }
//        }
//
//        val resultEmpty = realtime.publish("valid", "")
//        assertTrue("Invalid message should be published", resultEmpty)
//
//        val result = realtime.publish("valid", mapOf("key" to "value"))
//        assertTrue("Valid message should be published", result)
//
//    }
//
//    @Test
//    fun `history rejects invalid arguments`() = runTest {
//        val now = System.currentTimeMillis()
//
//        assertThrows(IllegalArgumentException::class.java) {
//            runBlocking {
//                realtime.history("", now, null)
//            }
//        }
//
//        assertThrows(IllegalArgumentException::class.java) {
//            runBlocking {
//                realtime.history("topic", now, now - 1000)
//            }
//        }
//
//        assertThrows(IllegalArgumentException::class.java) {
//            runBlocking {
//                realtime.history("topic", now, null)
//            }
//        }
//    }
//
//    @Test
//    fun `flush latency log executes correctly`() = runTest {
//        realtime.flushLatencyLogPublic(force = true)
//    }
//
//    @Test
//    fun `resend offline messages triggers MESSAGE_RESEND`() = runTest {
//        var triggered = false
//
//        val field = Realtime::class.java.getDeclaredField("sdkListeners")
//        field.isAccessible = true
//        val listeners = field.get(realtime) as ConcurrentHashMap<String, (Any) -> Unit>
//        listeners["MESSAGE_RESEND"] = { triggered = true }
//
//        runBlocking {
//            val offlineMessages = Collections.synchronizedList(mutableListOf<MutableMap<String, Any?>>())
//            offlineMessages.add(mutableMapOf("topic" to "offline.topic", "message" to "offline", "resent" to false))
//
//
//            realtime.offlineMessage(offlineMessages)
//        }
//
//        assertTrue(triggered)
//    }
//
//    @Test
//    fun `multiple on calls do not re-subscribe`() = runTest {
//        val topic = "dedupe-topic"
//        val listener: (JSONObject) -> Unit = {}
//
//        realtime.on(topic, listener)
//        realtime.on(topic, listener)
//        realtime.on(topic, listener)
//
//        assertEquals(1, realtime.listenersList().count { it.key == topic })
//    }

    @Test
    fun `Topic Validator`() = runTest {
        val unreservedInvalidTopics = mutableListOf(
            "\$foo",
            "foo$",
            "foo.$.bar",
            "foo..bar",
            ".foo",
            "foo.",
            "foo.>.bar",
            ">foo",
            "foo>bar",
            "foo.>bar",
            "foo.bar.>.",
            "foo bar",
            "foo/bar",
            "foo#bar",
            "",
            " ",
            "..",
            ".>",
            "foo..",
            ".",
            ">.",
            "foo,baz",
            "αbeta",
            "foo|bar",
            "foo;bar",
            "foo:bar",
            "foo%bar",
            "foo.*.>.bar",
            "foo.*.>.",
            "foo.*..bar",
            "foo.>.bar",
            "foo>"
        )

        for (topic in unreservedInvalidTopics) {
            val err = assertThrows(IllegalArgumentException::class.java) {
                realtimeEnabled.isTopicValid(topic)
            }

            assertEquals("Invalid topic", err.message)
        }

        val unreservedValidTopics = mutableListOf(
            "foo",
            "foo.bar",
            "foo.bar.baz",
            "*",
            "foo.*",
            "*.bar",
            "foo.*.baz",
            ">",
            "foo.>",
            "foo.bar.>",
            "*.*.>",
            "alpha_beta",
            "alpha-beta",
            "alpha~beta",
            "abc123",
            "123abc",
            "~",
            "alpha.*.>",
            "alpha.*",
            "alpha.*.*",
            "-foo",
            "foo_bar-baz~qux",
            "A.B.C",
            "sensor.temperature",
            "metric.cpu.load",
            "foo.*.*",
            "foo.*.>",
            "foo_bar.*",
            "*.*",
            "metrics.>"
        )

        for (topic in unreservedValidTopics) {
            println(topic)
            realtimeEnabled.isTopicValid(topic)
        }
    }

    @Test
    fun `Pattern Matcher Test`() = runTest {
        val cases: List<Triple<String, String, Boolean>> = listOf(
            Triple("foo",                 "foo",                      true),   // 1
            Triple("foo",                 "bar",                      false),  // 2
            Triple("foo.*",               "foo.bar",                  true),   // 3
            Triple("foo.bar",             "foo.*",                    true),   // 4
            Triple("*",                   "token",                    true),   // 5
            Triple("*",                   "*",                        true),   // 6
            Triple("foo.*",               "foo.bar.baz",              false),  // 7
            Triple("foo.>",               "foo.bar.baz",              true),   // 8
            Triple("foo.>",               "foo",                      false),  // 9
            Triple("foo.bar.baz",         "foo.>",                    true),   // 10
            Triple("foo.bar.>",           "foo.bar",                  false),  // 11
            Triple("foo",                 "foo.>",                    false),  // 12
            Triple("foo.*.>",             "foo.bar.baz.qux",          true),   // 13
            Triple("foo.*.baz",           "foo.bar.>",                true),   // 14
            Triple("alpha.*",             "beta.gamma",               false),  // 15
            Triple("alpha.beta",          "alpha.*.*",                false),  // 16
            Triple("foo.>.bar",           "foo.any.bar",              false),  // 17
            Triple(">",                   "foo.bar",                  true),   // 18
            Triple(">",                   ">",                        true),   // 19
            Triple("*",                   ">",                        true),   // 20
            Triple("*.>",                 "foo.bar",                  true),   // 21
            Triple("*.*.*",               "a.b.c",                    true),   // 22
            Triple("*.*.*",               "a.b",                      false),  // 23
            Triple("a.b.c.d.e",           "a.b.c.d.e",                true),   // 24
            Triple("a.b.c.d.e",           "a.b.c.d.f",                false),  // 25
            Triple("a.b.*.d",             "a.b.c.d",                  true),   // 26
            Triple("a.b.*.d",             "a.b.c.e",                  false),  // 27
            Triple("a.b.>",               "a.b",                      false),  // 28
            Triple("a.b",                 "a.b.c.d.>",               false),  // 29
            Triple("a.b.>.c",             "a.b.x.c",                  false),  // 30
            Triple("a.*.*",               "a.b",                      false),  // 31
            Triple("a.*",                 "a.b.c",                    false),  // 32
            Triple("metrics.cpu.load",    "metrics.*.load",           true),   // 33
            Triple("metrics.cpu.load",    "metrics.cpu.*",            true),   // 34
            Triple("metrics.cpu.load",    "metrics.>.load",           false),  // 35
            Triple("metrics.>",           "metrics",                  false),  // 36
            Triple("metrics.>",           "othermetrics.cpu",         false),  // 37
            Triple("*.*.>",               "a.b",                      false),  // 38
            Triple("*.*.>",               "a.b.c.d",                  true),   // 39
            Triple("a.b.c",               "*.*.*",                    true),   // 40
            Triple("a.b.c",               "*.*",                      false),  // 41
            Triple("alpha.*.>",           "alpha",                    false),  // 42
            Triple("alpha.*.>",           "alpha.beta",               false),  // 43
            Triple("alpha.*.>",           "alpha.beta.gamma",         true),   // 44
            Triple("alpha.*.>",           "beta.alpha.gamma",         false),  // 45
            Triple("foo-bar_baz",         "foo-bar_baz",              true),   // 46
            Triple("foo-bar_*",           "foo-bar_123",              false),  // 47
            Triple("foo-bar_*",           "foo-bar_*",                true),   // 48
            Triple("order-*",             "order-123",                false),  // 49
            Triple("hello.hey.*",         "hello.hey.>",              true)     // 50
        )

        cases.forEachIndexed { index, (tokenA, tokenB, expected) ->
            println("$tokenA  ⇆  $tokenB  → $expected")

            val result = realtimeEnabled.topicPatternMatcher(tokenA, tokenB)

            assertEquals(expected, result)
        }

    }
}
