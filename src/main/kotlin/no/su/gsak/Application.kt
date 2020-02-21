package no.su.gsak

import io.ktor.application.Application
import io.prometheus.client.CollectorRegistry

internal fun Application.sugsak() {
    val collectorRegistry = CollectorRegistry.defaultRegistry
    installMetrics(collectorRegistry)
    naisRoutes(collectorRegistry)
}

fun main(args: Array<String>) = io.ktor.server.netty.EngineMain.main(args)
