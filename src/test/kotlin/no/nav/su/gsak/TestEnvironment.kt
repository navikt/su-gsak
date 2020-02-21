package no.nav.su.gsak

import io.ktor.application.Application
import io.ktor.config.MapApplicationConfig
import io.ktor.util.KtorExperimentalAPI

@KtorExperimentalAPI
fun Application.testEnv() {
    (environment.config as MapApplicationConfig).apply {
        put("sts.username", "srvuser")
        put("sts.password", "srvpassword")
        put("kafka.username", "kafkaUser")
        put("kafka.password", "kafkaPassword")
        put("kafka.bootstrap", EmbeddedKafka.kafkaInstance.brokersURL)
        put("kafka.trustStorePath", "")
        put("kafka.trustStorePassword", "")
    }
}