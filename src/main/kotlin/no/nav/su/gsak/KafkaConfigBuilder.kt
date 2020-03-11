package no.nav.su.gsak

import io.ktor.config.ApplicationConfig
import io.ktor.util.KtorExperimentalAPI
import no.nav.su.meldinger.kafka.KafkaMiljø
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*

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

@KtorExperimentalAPI
internal class KafkaConfigBuilder(
    private val env: ApplicationConfig
) {
    private val LOG = LoggerFactory.getLogger(KafkaConfigBuilder::class.java)

    fun producerConfig() = kafkaBaseConfig().apply {
        put(ProducerConfig.ACKS_CONFIG, "all")
        put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    }

    private fun kafkaBaseConfig() = Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("kafka.bootstrap"))
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")
        val username = env.getProperty("kafka.username")
        val password = env.getProperty("kafka.password")
        put(
            SaslConfigs.SASL_JAAS_CONFIG,
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$username\" password=\"$password\";"
        )
        put(SaslConfigs.SASL_MECHANISM, "PLAIN")

        val truststorePath = env.getProperty("kafka.trustStorePath")
        val truststorePassword = env.getProperty("kafka.trustStorePassword")
        if (truststorePath != "" && truststorePassword != "")
            try {
                put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
                put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, File(truststorePath).absolutePath)
                put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword)
                LOG.info("Configured '${SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG}' location ")
            } catch (ex: Exception) {
                LOG.error("Failed to set '${SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG}' location", ex)
            }
    }

}