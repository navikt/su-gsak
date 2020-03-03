package no.nav.su.gsak

import io.ktor.application.Application
import io.ktor.config.ApplicationConfig
import io.ktor.util.KtorExperimentalAPI
import io.prometheus.client.CollectorRegistry
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import no.nav.su.gsak.KafkaConfigBuilder.Topics.SOKNAD_TOPIC
import no.nav.su.meldinger.kafka.MessageBuilder.Companion.fromConsumerRecord
import no.nav.su.meldinger.kafka.MessageBuilder.Companion.toProducerRecord
import no.nav.su.meldinger.kafka.headersAsString
import no.nav.su.meldinger.kafka.soknad.NySoknad
import no.nav.su.meldinger.kafka.soknad.NySoknadMedSkyggesak
import no.nav.su.meldinger.kafka.soknad.UkjentFormat
import no.nav.su.person.sts.StsConsumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Duration.of
import java.time.temporal.ChronoUnit.MILLIS
import java.util.*

val LOG = LoggerFactory.getLogger(Application::class.java)
const val xCorrelationId = "X-Correlation-ID"

@KtorExperimentalAPI
internal fun Application.sugsak(
        stsConsumer: StsConsumer = StsConsumer(
                environment.config.getProperty("sts.url"),
                environment.config.getProperty("serviceuser.username"),
                environment.config.getProperty("serviceuser.password")
        ),
        gsakConsumer: GsakConsumer = GsakConsumer(
                environment.config.getProperty("gsak.url"),
                stsConsumer
        )
) {
    Runtime.getRuntime().addShutdownHook(Thread(Runnable {
        LOG.error("Shutdown hook initiated - exiting application")
    }))

    val collectorRegistry = CollectorRegistry.defaultRegistry
    installMetrics(collectorRegistry)
    naisRoutes(collectorRegistry)


    val kafkaConfig = KafkaConfigBuilder(environment.config)
    val kafkaConsumer = KafkaConsumer(
            kafkaConfig.consumerConfig(),
            StringDeserializer(),
            StringDeserializer()
    ).also {
        it.subscribe(listOf(SOKNAD_TOPIC))
    }

    val kafkaProducer = KafkaProducer<String, String>(
            kafkaConfig.producerConfig(),
            StringSerializer(),
            StringSerializer()
    )

    launch {
        while (this.isActive) {
            kafkaConsumer.poll(of(100, MILLIS))
                    .map {
                        LOG.info("Polled event: topic:${it.topic()}, key:${it.key()}, value:${it.value()}: headers:${it.headersAsString()}")
                        when (val message = fromConsumerRecord(it)) {
                            is NySoknad -> {
                                gsakConsumer.hentGsak(
                                        message.sakId,
                                        message.aktoerId,
                                        it.headersAsString().getOrDefault(xCorrelationId, UUID.randomUUID().toString()))
                                        .also { gsakId ->
                                            kafkaProducer.send(NySoknadMedSkyggesak(
                                                    message.sakId,
                                                    message.aktoerId,
                                                    message.soknadId,
                                                    message.soknad,
                                                    gsakId
                                            ).toProducerRecord(SOKNAD_TOPIC, it.headersAsString()))
                                        }
                            }
                            is UkjentFormat -> {
                                LOG.warn("Unknown message format of type:${message::class}, message:$message")
                            }
                            else -> {
                                LOG.info("Skipping message of type:${message::class}")
                            }
                        }
                    }
        }
    }
}

fun main(args: Array<String>) = io.ktor.server.netty.EngineMain.main(args)

@KtorExperimentalAPI
internal fun ApplicationConfig.getProperty(key: String): String = property(key).getString()