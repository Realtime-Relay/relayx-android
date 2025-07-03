plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)

//    id("com.android.library")
//    id("org.jetbrains.kotlin.android")
    id("maven-publish")
}

android {
    namespace = "com.relay.realtime"
    compileSdk = 35

    defaultConfig {
        applicationId = "com.relay.realtime"
        minSdk = 26
        targetSdk = 35
        versionCode = 1
        versionName = "1.0"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
    kotlinOptions {
        jvmTarget = "11"
    }
}

dependencies {

    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.appcompat)
    implementation(libs.material)
    implementation(libs.androidx.activity)
    implementation(libs.androidx.constraintlayout)
    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)

//    implementation("io.nats:jnats:2.17.6") // Java NATS Client
    implementation("io.nats:jnats:2.20.5")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.2")


    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")
    implementation("org.msgpack:jackson-dataformat-msgpack:0.9.9") // latest Jan 2025


    implementation("com.google.code.gson:gson:2.10.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.1")
    implementation("org.msgpack:msgpack-core:0.9.0")

//    implementation("io.nats:jnats:2.17.6")
//
//    // WebSocket support
//    implementation("com.squareup.okhttp3:okhttp:4.12.0")
//
//    // Optional: Coroutines (recommended for async flow)
//    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3")
//
//    // Logging (optional)
//    implementation("com.jakewharton.timber:timber:5.0.1")

}