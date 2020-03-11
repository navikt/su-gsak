package no.nav.su.gsak

import io.ktor.application.Application
import io.ktor.config.ApplicationConfig
import io.ktor.util.KtorExperimentalAPI
import io.prometheus.client.CollectorRegistry
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import no.nav.su.gsak.Metrics.messageProcessed
import no.nav.su.gsak.Metrics.messageRead
import no.nav.su.meldinger.kafka.Topics.SØKNAD_TOPIC
import no.nav.su.meldinger.kafka.headersAsString
import no.nav.su.meldinger.kafka.soknad.NySøknad
import no.nav.su.meldinger.kafka.soknad.NySøknadMedSkyggesak
import no.nav.su.meldinger.kafka.soknad.SøknadMelding
import no.nav.su.meldinger.kafka.soknad.SøknadMelding.Companion.fromConsumerRecord
import no.nav.su.person.sts.StsConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Duration.of
import java.time.temporal.ChronoUnit.MILLIS
import java.util.*
import kotlin.system.exitProcess

val LOG = LoggerFactory.getLogger(Application::class.java)
const val xCorrelationId = "X-Correlation-ID"

@KtorExperimentalAPI
internal fun Application.suGsak(
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
    val collectorRegistry = CollectorRegistry.defaultRegistry
    installMetrics(collectorRegistry)
    naisRoutes(collectorRegistry)

    val kafkaConfig = KafkaConfigBuilder(environment.config)
    val kafkaConsumer = KafkaConsumer(
            kafkaConfig.consumerConfig(),
            StringDeserializer(),
            StringDeserializer()
    ).also {
        it.subscribe(listOf(SØKNAD_TOPIC))
    }

    val kafkaProducer = KafkaProducer<String, String>(
            kafkaConfig.producerConfig(),
            StringSerializer(),
            StringSerializer()
    )

    Runtime.getRuntime().addShutdownHook(Thread(Runnable {
        LOG.error("Shutdown hook initiated - exiting application")
    }))

    val useGSak = environment.config.getProperty("gsak.enabled").toBoolean()

    launch {
        try {
            while (this.isActive) {
                kafkaConsumer.poll(of(100, MILLIS))
                    .onEach {
                        it.logMessage()
                        messageRead()
                    }
                    .filter { fromConsumerRecord(it) is NySøknad }
                    .map { fromConsumerRecord(it) as NySøknad }
                    .forEach {message ->
                        if (useGSak) {
                            val gsakId = gsakConsumer.hentGsak(message)
                            kafkaProducer.send(message.medSkyggesak(gsakId).toProducerRecord(SØKNAD_TOPIC))
                            messageProcessed()
                        } else {
                            LOG.info(message.toString())
                            messageProcessed()
                        }
                    }
            }
        } catch (e: Exception) {
            LOG.error("Exception caught while processing kafka message: ", e)
            exitProcess(1)
        }
    }
}

fun ConsumerRecord<String, String>.logMessage() {
    LOG.info("Polled message: topic:${this.topic()}, key:${this.key()}, value:${this.value()}: $xCorrelationId:${this.headersAsString()[xCorrelationId]}")
}

fun main(args: Array<String>) = io.ktor.server.netty.EngineMain.main(args)

@KtorExperimentalAPI
internal fun ApplicationConfig.getProperty(key: String): String = property(key).getString()