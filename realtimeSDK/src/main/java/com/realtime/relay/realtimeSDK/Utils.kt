package com.realtime.relay.realtimeSDK

import android.content.Context
import java.io.File
import java.io.FileOutputStream

object Utils {
    val API_KEY = "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJhdWQiOiJOQVRTIiwibmFtZSI6IjY4NTNhOGMzNzFmY2JhNzNlMThiMGNjYyIsInN1YiI6IlVEUkREUkpXQUcyRFdPRlZWTlc1SDczSzc3U0NXUVVLQTVFTlFKU1lGN1FUNVQ1QVNQVTdYNUhSIiwibmF0cyI6eyJkYXRhIjotMSwicGF5bG9hZCI6LTEsInN1YnMiOi0xLCJwdWIiOnsiZGVueSI6WyI-Il19LCJzdWIiOnsiZGVueSI6WyI-Il19LCJvcmdfZGF0YSI6eyJvcmdfaWQiOiI2ODUwMDQ1ZWJlYzE5ZjI3NGNiZmI2MGEiLCJvcmdfbmFtZSI6IlNwYWNlWCIsInZhbGlkaXR5X2tleSI6IjNjYTg3OWY1LTAxN2UtNDQzMC1iNzczLTVhNTNiZDA5NmU2NSIsInJvbGUiOiJ1c2VyIiwicHJvamVjdF9pZCI6IjY4NTNhOGMzNzFmY2JhNzNlMThiMGNjYyJ9LCJpc3N1ZXJfYWNjb3VudCI6IkFDSktNTUs2VFBJNUtYWDY0NExBSEpPVk9BRkNYWlRSS0dCVVcyQzQ0TkhESFRWTFo3T1ZKRDdLIiwidHlwZSI6InVzZXIiLCJ2ZXJzaW9uIjoyfSwiaXNzIjoiQUNKS01NSzZUUEk1S1hYNjQ0TEFISk9WT0FGQ1haVFJLR0JVVzJDNDROSERIVFZMWjdPVkpEN0siLCJpYXQiOjE3NTI4MTAwNjEsImp0aSI6IjloUnUzYitqV3N4M0lQbzJ2dCtZT0N1Y2Yvbm1oNS9BZm0xbXVra0thM3B5czhSeC8xZE5UTWliblFsZzh5VUZ4NVR3RzBiZ3Q3U212aENNVVY5Um5BPT0ifQ.19GumHD_RI_V4XRYJxXNxQ8Chmg-wg_9ec9upotvpZ8fZ4oynmmjPLHnZJn1oLXYhzpFxus4FIOKk50r1JG7Dg"
    val SECRET_KEY = "SUAA3D5D62VX5D45U6C2JDC7UG2DJ2WUO3FMIEK4AKI4B5AVAU3TKIA5RU"


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