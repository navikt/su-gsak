package no.nav.su.gsak

import io.ktor.config.ApplicationConfig
import io.ktor.util.KtorExperimentalAPI
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.temporal.ChronoUnit

@KtorExperimentalAPI
internal class Gsak(
    private val env: ApplicationConfig
) {
    private val LOG = LoggerFactory.getLogger(Gsak::class.java)
    val kafkaConfig = KafkaConfigBuilder(env)
    val kafkaConsumer = KafkaConsumer(
        kafkaConfig.consumerConfig(),
        StringDeserializer(),
        StringDeserializer()
    ).also {
        it.subscribe(listOf(KafkaConfigBuilder.Topics.SOKNAD_TOPIC))
    }

    val kafkaProducer = KafkaProducer<String, String>(
        kafkaConfig.producerConfig(),
        StringSerializer(),
        StringSerializer()
    )

    fun lesHendelser() {
        GlobalScope.launch {
            while (true) {
                val records: ConsumerRecords<String, String> = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS))
                records.map {
                    LOG.info("Polled event: topic:${it.topic()}, key:${it.key()}, value:${it.value()}")
                }
            }
        }
    }
}