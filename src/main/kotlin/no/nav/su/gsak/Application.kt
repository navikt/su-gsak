package no.nav.su.gsak

import io.ktor.application.Application
import io.ktor.config.ApplicationConfig
import io.ktor.util.KtorExperimentalAPI
import io.prometheus.client.CollectorRegistry
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import no.nav.su.gsak.KafkaConfigBuilder.Topics.SOKNAD_TOPIC
import no.nav.su.gsak.Metrics.messageProcessed
import no.nav.su.gsak.Metrics.messageRead
import no.nav.su.meldinger.kafka.headersAsString
import no.nav.su.meldinger.kafka.soknad.NySøknad
import no.nav.su.meldinger.kafka.soknad.NySøknadMedSkyggesak
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
        it.subscribe(listOf(SOKNAD_TOPIC))
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
                    .map { Pair(fromConsumerRecord(it) as NySøknad, it.headersAsString()) }
                    .forEach {
                        val message = it.first
                        if (useGSak) {
                            val correlationId = it.second.getOrDefault(xCorrelationId, UUID.randomUUID().toString())
                            val gsakId = gsakConsumer.hentGsak(message.sakId, message.aktørId, correlationId)
                            kafkaProducer.send(message.asSkygge(gsakId).toProducerRecord(SOKNAD_TOPIC, it.second))
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

private fun NySøknad.asSkygge(gsakId: String) = NySøknadMedSkyggesak(sakId = sakId, aktørId = aktørId, søknadId = søknadId, søknad = søknad, fnr = fnr, gsakId = gsakId)

fun ConsumerRecord<String, String>.logMessage() {
    LOG.info("Polled message: topic:${this.topic()}, key:${this.key()}, value:${this.value()}: $xCorrelationId:${this.headersAsString()[xCorrelationId]}")
}

fun main(args: Array<String>) = io.ktor.server.netty.EngineMain.main(args)

@KtorExperimentalAPI
internal fun ApplicationConfig.getProperty(key: String): String = property(key).getString()