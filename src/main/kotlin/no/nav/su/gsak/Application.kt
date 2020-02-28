package no.nav.su.gsak

import io.ktor.application.Application
import io.ktor.config.ApplicationConfig
import io.ktor.util.KtorExperimentalAPI
import io.prometheus.client.CollectorRegistry
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import no.nav.su.person.sts.StsConsumer
import org.slf4j.LoggerFactory
import java.lang.RuntimeException
import kotlin.system.exitProcess

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

    GlobalScope.launch {
        try {
            throw RuntimeException("some crazy error")
        } catch (e: Exception) {
            LOG.error("Exception caught while processing - initiating shutdown hook. Exception: $e")
            cancel("Exception caught while processing - canceling coroutine", e)
            exitProcess(1)
        }
    }

    val collectorRegistry = CollectorRegistry.defaultRegistry
    installMetrics(collectorRegistry)
    naisRoutes(collectorRegistry)

    /*
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

    fun prosesserHendelser() {
        GlobalScope.launch {
            while (true) {
                val records: ConsumerRecords<String, String> = kafkaConsumer.poll(of(100, MILLIS))
                records.filter { compatible(it, NySoknad::class.java) }
                        .map {
                            LOG.info("Polled event: topic:${it.topic()}, key:${it.key()}, value:${it.value()}: headers:${it.headersAsString()}")
                            val nySoknad = fromConsumerRecord(it, NySoknad::class.java)
                            gsakConsumer.hentGsak(
                                    nySoknad.sakId,
                                    nySoknad.aktoerId,
                                    it.headersAsString().getOrDefault(xCorrelationId, UUID.randomUUID().toString())).also { gsakId ->
                                kafkaProducer.send(toProducerRecord(SOKNAD_TOPIC, NySoknadHentGsak(
                                        nySoknad.sakId,
                                        nySoknad.aktoerId,
                                        nySoknad.soknadId,
                                        nySoknad.soknad,
                                        gsakId
                                ), it.headersAsString()))
                            }
                        }
            }
        }
    }
    prosesserHendelser()
     */
}

fun main(args: Array<String>) = io.ktor.server.netty.EngineMain.main(args)

@KtorExperimentalAPI
internal fun ApplicationConfig.getProperty(key: String): String = property(key).getString()