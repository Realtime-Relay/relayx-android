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

    testImplementation("junit:junit:4.13.2")
    testImplementation("org.mockito:mockito-core:5.11.0")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.2.1")
    testImplementation("org.robolectric:robolectric:4.12.1") // if needed



//    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
//    testImplementation("io.mockk:mockk:1.13.8") // or Mockito if preferred
//    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.7.3")
//
//
////    testImplementation("io.mockk:mockk:1.13.5")
////    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.7.3")
//    testImplementation("junit:junit:4.13.2")
//    testImplementation("androidx.test:core:1.5.0")
//    testImplementation("org.robolectric:robolectric:4.10.3")
////    testImplementation(kotlin("test"))
    testImplementation(kotlin("test"))


}