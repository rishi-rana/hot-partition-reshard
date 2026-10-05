plugins {
    java
    application
}

group = "com.hotpartition"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:4.3.0")
    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation("org.testcontainers:kafka:1.21.3")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.19")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.testcontainers:kafka:1.21.3")
    testImplementation("org.testcontainers:junit-jupiter:1.21.3")
    testImplementation("org.apache.kafka:kafka-clients:4.3.0")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

application {
    mainClass.set("com.hotpartition.eval.Main")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}
