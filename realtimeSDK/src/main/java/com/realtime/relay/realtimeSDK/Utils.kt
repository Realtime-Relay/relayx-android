package com.realtime.relay.realtimeSDK

import android.content.Context
import java.io.File
import java.io.FileOutputStream

object Utils {
    val API_KEY = "<API Key>"
    val SECRET_KEY = "<Secret Key>"


    fun createNatsCredsFile(context: Context, jwt: String, nkeySeed: String): File {
        val filename = "nats_user.creds"

        val fileContent = """
        -----BEGIN NATS USER JWT-----
        $jwt
        ------END NATS USER JWT------

        ************************* IMPORTANT *************************
        NKEY Seed printed below can be used to sign and prove identity.
        NKEYs are sensitive and should be treated as secrets.

        -----BEGIN USER NKEY SEED-----
        $nkeySeed
        ------END USER NKEY SEED------

        *************************************************************
    """.trimIndent()

        // Save to internal storage (private to the app)
        val file = File(context.filesDir, filename)
        FileOutputStream(file).use { output ->
            output.write(fileContent.toByteArray(Charsets.UTF_8))
        }

        return file
    }

}