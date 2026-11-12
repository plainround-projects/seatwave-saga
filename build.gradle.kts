// seatwave-saga (Module 28). Versions are in gradle/libs.versions.toml, checked on Maven Central
// on 12 November 2026.
plugins {
    java
    jacoco
    alias(libs.plugins.spotless)
}

group = "io.plainround"
version = "1.0.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

dependencies {
    // The saga, the relay model, the read model, the two-phase commit model and the payment
    // provider stub need only the JDK. The adapters under kafka, jdbc and stack need what the JDK
    // lacks: Kafka's protocol, PostgreSQL's wire protocol and a pool, and a JSON reader and writer
    // for the outbox payloads and the services' HTTP bodies.
    implementation(libs.kafka.clients)
    constraints {
        // kafka-clients 4.4.0 asks for versions OSV lists with advisories (GHSA-mcr4-qmvw-px4g and
        // two more for lz4-java 1.11.2; GHSA-gwq9-9ffj-mvrv and two more for snappy-java 1.1.10.7),
        // checked 12 November 2026. Both compressors are used only when a producer compresses.
        implementation(libs.lz4.java) { because("lz4-java 1.11.2 has advisories; fixed in 1.11.4") }
        implementation(libs.snappy.java) { because("snappy-java 1.1.10.7 has advisories; fixed in 1.1.10.9") }
    }
    implementation(libs.postgresql)
    implementation(libs.hikari)
    implementation(platform(libs.jackson.bom))
    implementation(libs.jackson.databind)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    // The stack tests wait for "the relay published it" without a guessed sleep.
    testImplementation(libs.awaitility)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Transitive versions are written to gradle.lockfile, so two builds can never resolve different jars.
dependencyLocking {
    lockAllConfigurations()
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release = 25
    // Every warning is an error: a warning that is allowed to stay is a defect nobody owns.
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

spotless {
    java {
        googleJavaFormat(libs.versions.googleJavaFormat.get())
        removeUnusedImports()
        endWithNewline()
    }
}

jacoco {
    toolVersion = libs.versions.jacoco.get()
}

// The tests tagged "stack" need the broker, the three PostgreSQL databases and the payment
// provider stub of compose.yaml; ./gradlew check never runs them, so a reader without Docker can
// still build and test the rest. Deferred triggers, partial unique indexes, the relay's fence and
// Kafka's per-partition order are the subject, so no embedded stand-in replaces them.
tasks.test {
    useJUnitPlatform {
        excludeTags("stack")
    }
    testLogging {
        events("passed", "failed", "skipped")
    }
    finalizedBy(tasks.jacocoTestReport)
}

tasks.register<Test>("stackTest") {
    group = "verification"
    description = "Runs the tests against the lab stack; scripts/stack.sh test starts and stops it."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform {
        includeTags("stack")
    }
    systemProperty("seatwave.stackSettings", file("config/stack.properties").absolutePath)
    // The stack is outside Gradle's view, so these tests are never up to date.
    outputs.upToDateWhen { false }
    testLogging {
        events("passed", "failed", "skipped")
    }
}

// The adapters run only against the stack, and the programs are run by hand: both are measured by
// the stack run, not by ./gradlew check.
val stackOnly = listOf("**/jdbc/**", "**/kafka/**", "**/stack/**", "**/*Cli*")
val programs = listOf("**/*Cli*")

tasks.jacocoTestReport {
    reports {
        xml.required = true
    }
}

tasks.jacocoTestCoverageVerification {
    dependsOn(tasks.test)
    classDirectories.setFrom(sourceSets.main.get().output.asFileTree.matching { exclude(stackOnly) })
    violationRules {
        rule {
            limit {
                counter = "LINE"
                minimum = "0.80".toBigDecimal()
            }
        }
    }
}

// The same floor over everything but the programs, with both test runs' data; scripts/stack.sh test
// runs it.
tasks.register<JacocoReport>("stackCoverageReport") {
    group = "verification"
    description = "Writes the line coverage of the unit and stack test runs together."
    dependsOn(tasks.test, tasks.named("stackTest"))
    executionData(fileTree(layout.buildDirectory.dir("jacoco")).include("*.exec"))
    sourceDirectories.setFrom(sourceSets.main.get().allSource.srcDirs)
    classDirectories.setFrom(sourceSets.main.get().output.asFileTree.matching { exclude(programs) })
    reports {
        xml.required = true
    }
}

tasks.register<JacocoCoverageVerification>("stackCoverage") {
    group = "verification"
    description = "Checks line coverage of all main code but the programs over both test runs."
    dependsOn(tasks.named("stackCoverageReport"))
    executionData(fileTree(layout.buildDirectory.dir("jacoco")).include("*.exec"))
    classDirectories.setFrom(sourceSets.main.get().output.asFileTree.matching { exclude(programs) })
    violationRules {
        rule {
            limit {
                counter = "LINE"
                minimum = "0.80".toBigDecimal()
            }
        }
    }
}

tasks.check {
    dependsOn(tasks.jacocoTestCoverageVerification)
}

// One task per program, so a reader runs one command and sees one result. The first five need only
// the JDK; the stack's services and drill tools are started by scripts/drill.sh with plain java, so
// that a drill can kill one by its process id.
mapOf(
    "dualWriteRun" to "io.plainround.seatwave.saga.DualWriteCli",
    "sagaRun" to "io.plainround.seatwave.saga.SagaCli",
    "choreographyRun" to "io.plainround.seatwave.saga.ChoreographyCli",
    "twoPhaseCommitRun" to "io.plainround.seatwave.saga.TwoPhaseCommitCli",
    "readModelRun" to "io.plainround.seatwave.saga.ReadModelCli",
).forEach { (taskName, programClass) ->
    tasks.register<JavaExec>(taskName) {
        group = "seatwave"
        classpath = sourceSets["main"].runtimeClasspath
        mainClass = programClass
    }
}

// The drills start the services and tools with plain java, in the background. This task writes the
// class path they need as one line.
tasks.register("drillClasspath") {
    group = "seatwave"
    description = "Compiles the code and writes build/drill/runtime.classpath for scripts/drill.sh."
    val runtime = sourceSets["main"].runtimeClasspath
    val target = layout.buildDirectory.file("drill/runtime.classpath")
    inputs.files(runtime)
    outputs.file(target)
    doLast {
        target.get().asFile.writeText(runtime.files.joinToString(File.pathSeparator, postfix = "\n") { it.absolutePath })
    }
}
