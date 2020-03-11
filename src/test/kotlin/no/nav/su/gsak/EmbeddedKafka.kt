package no.nav.su.gsak

import io.ktor.util.KtorExperimentalAPI
import no.nav.common.KafkaEnvironment
import no.nav.su.meldinger.kafka.Topics.SØKNAD_TOPIC
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.*

@KtorExperimentalAPI
class EmbeddedKafka {
    companion object {
        fun embeddedKafka() = KafkaEnvironment(
                autoStart = true,
                noOfBrokers = 1,
                topicInfos = listOf(KafkaEnvironment.TopicInfo(name = SØKNAD_TOPIC, partitions = 1)),
                withSchemaRegistry = false,
                withSecurity = false,
                brokerConfigOverrides = Properties().apply {
                    this["auto.leader.rebalance.enable"] = "false"
                    this["group.initial.rebalance.delay.ms"] = "1" //Avoid waiting for new consumers to join group before first rebalancing (default 3000ms)
                }
        )

        fun testKafkaConsumer(brokersURL: String) = KafkaConsumer(
                testConsumerProperties(brokersURL),
                StringDeserializer(),
                StringDeserializer()).also {
            it.subscribe(listOf(SØKNAD_TOPIC))
        }

        private fun testConsumerProperties(brokersURL: String): MutableMap<String, Any>? {
            return HashMap<String, Any>().apply {
                put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokersURL)
                put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")
                put(SaslConfigs.SASL_MECHANISM, "PLAIN")
                put(ConsumerConfig.GROUP_ID_CONFIG, "test")
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100")
            }
        }
    }
}