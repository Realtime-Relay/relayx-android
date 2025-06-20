package com.angistudio.nata_sdk

import android.os.Bundle
import androidx.activity.enableEdgeToEdge
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat

class MainActivity : AppCompatActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContentView(R.layout.activity_main)
        ViewCompat.setOnApplyWindowInsetsListener(findViewById(R.id.main)) { v, insets ->
            val systemBars = insets.getInsets(WindowInsetsCompat.Type.systemBars())
            v.setPadding(systemBars.left, systemBars.top, systemBars.right, systemBars.bottom)
            insets
        }

        initUI()
    }

    private fun initUI() {
        val realtime = Realtime("eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJhdWQiOiJOQVRTIiwibmFtZSI6IklPUyBEZXYiLCJzdWIiOiJVQU9STjRWQkNXQzJORU1FVkpFWUY3VERIUVdYTUNLTExTWExNTjZRTjRBVU1WUElDSVJOSEpJRyIsIm5hdHMiOnsiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJzdWJzIjotMSwicHViIjp7ImRlbnkiOlsiPiJdfSwic3ViIjp7ImRlbnkiOlsiPiJdfSwib3JnX2RhdGEiOnsib3JnYW5pemF0aW9uIjoicmVsYXktaW50ZXJuYWwiLCJwcm9qZWN0IjoiSU9TIERldiJ9LCJpc3N1ZXJfYWNjb3VudCI6IkFDWklKWkNJWFNTVVU1NVlFR01QMjM2TUpJMkNSSVJGRkdJRDRKVlE2V1FZWlVXS08yVTdZNEJCIiwidHlwZSI6InVzZXIiLCJ2ZXJzaW9uIjoyfSwiaXNzIjoiQUNaSUpaQ0lYU1NVVTU1WUVHTVAyMzZNSkkyQ1JJUkZGR0lENEpWUTZXUVlaVVdLTzJVN1k0QkIiLCJpYXQiOjE3NDUwNTE2NjcsImp0aSI6IllVMG50TXFNcHhwWFNWbUp0OUJDazhhV0dxd0NwYytVQ0xwa05lWVBVcDNNRTNQWDBRcUJ2ZjBBbVJXMVRDamEvdTg2emIrYUVzSHVKUFNmOFB2SXJnPT0ifQ._LtZJnTADAnz3N6U76OaA-HCYq-XxckChk1WlHi_oZXfYP2vqcGIiNDFSQ-XpfjUTfKtXEuzcf_BDq54nSEMAA", "SUAPWRWRITWYL4YP7B5ZHU3W2G2ZPYJ47IN4UWNHLMFTSIJEOMQJWWSWGY")

        Thread {
            realtime.connect()
            realtime.publish("foo", "bar")
            realtime.close()

        }.start()

    }
}