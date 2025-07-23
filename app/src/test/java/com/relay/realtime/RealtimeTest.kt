package com.relay.realtime.realtimeSDK

import android.content.Context
import com.google.gson.JsonObject
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.json.JSONObject
import org.junit.*
import org.junit.Assert.*
import org.junit.runner.RunWith
import org.mockito.Mock
import org.mockito.junit.MockitoJUnitRunner
import org.robolectric.shadows.ShadowLog
import java.lang.Exception
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.mapOf
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

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
    }

    @Test
    fun `No API & Secret Key`() {
        val exception = assertThrows(IllegalArgumentException::class.java) {
            Realtime(context = context, apiKey = "", secretKey = "")
        }
        assertEquals("apiKey must not be empty", exception.message)
    }

    @Test
    fun `Only API Key Passed`() {
        val exception = assertThrows(IllegalArgumentException::class.java) {
            Realtime(context = context, apiKey = "KEY", secretKey = "")
        }
        assertEquals("secretKey must not be empty", exception.message)
    }

    @Test
    fun `Only Secret Key Passed`() {
        val exception = assertThrows(IllegalArgumentException::class.java) {
            Realtime(context = context, apiKey = "", secretKey = "SECRET")
        }
        assertEquals("apiKey must not be empty", exception.message)
    }

    @Test
    fun `Null Keys Passed to Constructor`() {
        assertThrows(NullPointerException::class.java) {
            Realtime(context = context, apiKey = null!!, secretKey = null!!)
        }
    }

    @Test
    fun `init() Test with Multiple Configurations`() = runBlocking {
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
        runBlocking {
            realtimeEnabled = Realtime(
                context = context,
                apiKey = apiKey,
                secretKey = secretKey,
                callbackDispatcher = Dispatchers.Unconfined
            )
            realtimeEnabled.init(staging = staging, opts = mapOf("debug" to true))
            realtimeEnabled.connect()

            // Delay so that the connection takes place & getNamespace() is called
            delay(5000)

            assertTrue(realtimeEnabled.checkIsConnected())

            assertTrue(realtimeEnabled.getNamespaceTest()?.length!! > 0)
            assertTrue(realtimeEnabled.getHashTest()?.length!! > 0)

            realtimeEnabled.close()
        }
    }

    @Test
    fun `Publish Test (Valid inputs & online)`() = runTest {
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

        runBlocking {
            realtimeEnabled = Realtime(
                context = context,
                apiKey = apiKey,
                secretKey = secretKey,
                callbackDispatcher = Dispatchers.Unconfined
            )
            realtimeEnabled.init(staging = staging, opts = mapOf("debug" to true))
            realtimeEnabled.connect()

            delay(5000)

            for (topic in unreservedValidTopics) {
                var sent = realtimeEnabled.publish(topic, mapOf("data" to "Hello World!"))

                assertTrue(sent)

                sent = realtimeEnabled.publish(topic, "HEY!")
                assertTrue(sent)

                sent = realtimeEnabled.publish(topic, 1234)
                assertTrue(sent)

                sent = realtimeEnabled.publish(topic, 12.34)
                assertTrue(sent)
            }

            realtimeEnabled.close()

        }
    }

    @Test
    fun `Publish Test Invalid Inputs`() = runTest {
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

        for(topic in unreservedInvalidTopics){
            val exception = assertThrows(IllegalArgumentException::class.java) {
                runBlocking {
                    val realtime = Realtime(context = context, apiKey = "<KEY>", secretKey = "<KEY>")

                    realtime.publish(topic, "hey")
                }
            }
            assertEquals("Invalid topic", exception.message)
        }

        val invalidMessages = mutableListOf(
            true,                           // Boolean
            listOf(1, 2, 3),                // List
            arrayOf("a", "b"),              // Array
            setOf(1.0, 2.0),                // Set
            Pair(1, "two"),                 // Pair / Tuple
            ByteArray(4),                   // Byte array (raw binary)
            Unit,                           // Kotlin Unit object
            object { val x = 1 },           // Anonymous object
            IllegalStateException("boom"),  // Throwable / arbitrary class
        )

        for(message in invalidMessages){
            val exception = assertThrows(Exception::class.java) {
                runBlocking {
                    val realtime = Realtime(context = context, apiKey = "<KEY>", secretKey = "<KEY>")

                    println(message)
                    realtime.publish("topic", message!!)
                }
            }
            assertEquals("Message must be string, number or Map<String, String | Number | Map>", exception.message)
        }
    }

    @Test
    fun `Publish Offline Mode`() = runTest {
        runBlocking {
            val realtime = Realtime(context = context, apiKey = "<KEY>", secretKey = "<KEY>")

            var sent = realtime.publish("topic", "Hey what's up?")
            assertFalse(sent)
        }
    }

    @Test
    fun `on() Valid Test`() = runTest {
        val reserved = listOf("CONNECTED", "RECONNECT", "DISCONNECTED", "MESSAGE_RESEND")
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

        runBlocking {
            val realtime = Realtime(context = context, apiKey = "<KEY>", secretKey = "<KEY>")

            for(topic in reserved){
                var init = realtime.on(topic, {})
                assertTrue(init)
            }

            // Running it again but now it should be false since the topics are already initialized
            for(topic in reserved){
                var init = realtime.on(topic, {})
                assertFalse(init)
            }

            for(topic in unreservedValidTopics){
                var init = realtime.on(topic, {})
                assertTrue(init)
            }

            // Running it again but now it should be false since the topics are already initialized
            for(topic in unreservedValidTopics){
                var init = realtime.on(topic, {})
                assertFalse(init)
            }
        }
    }

    @Test
    fun `on() Invalid Test`() = runTest {
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

        runBlocking {
            val realtime = Realtime(context = context, apiKey = "<KEY>", secretKey = "<KEY>")

            for(topic in unreservedInvalidTopics){
                val exception = assertThrows(IllegalArgumentException::class.java) {
                    runBlocking {
                        realtime.on(topic, {})
                    }
                }
                assertEquals("Invalid topic", exception.message)
            }
        }
    }

    @Test
    fun `off() Valid Test`() = runTest {
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

        runBlocking {
            val realtime = Realtime(context = context, apiKey = "<KEY>", secretKey = "<KEY>")

            for(topic in unreservedValidTopics){
                realtime.off(topic)
            }

            var init = realtime.on(Realtime.CONNECTED, {})
            assertTrue(init)

            var off = realtime.off(Realtime.CONNECTED)
            assertTrue(off)
        }
    }

    @Test
    fun `off() Invalid Test`() = runTest {
        val unreservedValidTopics = mutableListOf(
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

        runBlocking {
            val realtime = Realtime(context = context, apiKey = "<KEY>", secretKey = "<KEY>")

            for(topic in unreservedValidTopics){
                val exception = assertThrows(IllegalArgumentException::class.java) {
                    realtime.off(topic)
                }
                assertEquals("Invalid topic", exception.message)
            }
        }
    }

    @Test
    fun `History Test`() = runTest {
        runBlocking {
            realtimeEnabled = Realtime(
                context = context,
                apiKey = apiKey,
                secretKey = secretKey,
                callbackDispatcher = Dispatchers.Unconfined
            )
            realtimeEnabled.init(staging = staging, opts = mapOf("debug" to true))

            val since = System.currentTimeMillis() - 5 * 60 * 60 * 1_000
            val end = System.currentTimeMillis()
            var result = realtimeEnabled.history("hello", since, end)

            assertTrue(result.size == 0)

            var exception = assertThrows(Exception::class.java) {
                runBlocking {
                    realtimeEnabled.history("hello", end, since)
                }
            }
            assertEquals("End date <= start date", exception.message)

            realtimeEnabled.connect()

            // Delay so that the connection takes place & getNamespace() is called
            delay(5000)

            assertTrue(realtimeEnabled.checkIsConnected())

            realtimeEnabled.close()
        }
    }

    @Test
    fun `Topic Validator`() = runTest {
        val realtime = Realtime(context, apiKey, secretKey)

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
                realtime.isTopicValid(topic)
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
            realtime.isTopicValid(topic)
        }
    }

    @Test
    fun `Pattern Matcher Test`() = runTest {
        val realtime = Realtime(context, apiKey, secretKey)

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

            val result = realtime.topicPatternMatcher(tokenA, tokenB)

            assertEquals(expected, result)
        }

    }
}
