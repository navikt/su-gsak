package no.nav.su.gsak

import io.prometheus.client.Counter

object Metrics {
    private val messagesRead = Counter.build("messages_read_total", "Antall meldinger lest fra kafka").register()
    private val messagesProcessed = Counter.build("messages_processed_total", "Antall meldinger prosessert").register()
    private val messagesUnknownFormat = Counter.build("messages_skipped_unknown_format_total", "Antall meldinger med ukjent format").register()
    private val messagesSkipped = Counter.build("messages_skipped_total", "Antall meldinger av typer som ikke skal prosesseres").register()

    fun messageRead(){
        messagesRead.inc()
    }

    fun messageProcessed(){
        messagesProcessed.inc()
    }

    fun messageUnknownFormat(){
        messagesUnknownFormat.inc()
    }

    fun messageSkipped(){
        messagesSkipped.inc()
    }
}