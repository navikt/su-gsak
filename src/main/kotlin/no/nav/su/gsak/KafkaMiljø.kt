package no.nav.su.gsak

import io.ktor.config.ApplicationConfig
import io.ktor.util.KtorExperimentalAPI
import no.nav.su.meldinger.kafka.KafkaMiljø

@KtorExperimentalAPI
fun ApplicationConfig.kafkaMiljø() = KafkaMiljø(
    groupId = getProperty("kafka.groupId"),
    username = getProperty("kafka.username"),
    password = getProperty("kafka.password"),
    trustStorePath = getProperty("kafka.username"),
    trustStorePassword = getProperty("kafka.trustStorePassword"),
    commitInterval = getProperty("kafka.commitInterval"),
    bootstrap = getProperty("kafka.bootstrap")
)
