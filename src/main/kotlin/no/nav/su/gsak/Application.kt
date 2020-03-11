package no.nav.su.gsak

import io.ktor.application.Application
import io.ktor.config.ApplicationConfig
import io.ktor.util.KtorExperimentalAPI
import io.prometheus.client.CollectorRegistry
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import no.nav.su.meldinger.kafka.Topics.SØKNAD_TOPIC
import no.nav.su.meldinger.kafka.soknad.NySøknad
import org.slf4j.LoggerFactory
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

    val kafkaProducer = environment.config.kafkaMiljø().producer()

    Runtime.getRuntime().addShutdownHook(Thread(Runnable {
        LOG.error("Shutdown hook initiated - exiting application")
    }))

    val useGSak = environment.config.getProperty("gsak.enabled").toBoolean()
    val meldingsleser = environment.config.kafkaMiljø().meldingsleser(Metrics)
    launch {
        try {
            while (this.isActive) {
                meldingsleser.lesMelding<NySøknad> { message ->
                    if (useGSak) {
                        val gsakId = gsakConsumer.hentGsak(message)
                        kafkaProducer.send(message.medSkyggesak(gsakId).toProducerRecord(SØKNAD_TOPIC))
                    } else {
                        LOG.info(message.toString())
                    }
                }
            }
        } catch (e: Exception) {
            LOG.error("Exception caught while processing kafka message: ", e)
            exitProcess(1)
        }
    }
}

fun main(args: Array<String>) = io.ktor.server.netty.EngineMain.main(args)

@KtorExperimentalAPI
internal fun ApplicationConfig.getProperty(key: String): String = property(key).getString()