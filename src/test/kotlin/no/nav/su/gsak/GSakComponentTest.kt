package no.nav.su.gsak

import io.ktor.server.testing.withTestApplication
import io.ktor.util.KtorExperimentalAPI
import no.nav.su.gsak.KafkaConfigBuilder.Topics.SOKNAD_TOPIC
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test

@KtorExperimentalAPI
class GSakComponentTest {

    @Test
    fun `gitt at vi ikke har en skyggesak fra før av skal vi lage en ny skyggesak når vi får melding om ny sak`(){
        withTestApplication({
            testEnv()
            sugsak()
        }) {
            val kafkaConfig = KafkaConfigBuilder(environment.config)
            val producer = KafkaProducer(kafkaConfig.producerConfig(), StringSerializer(), StringSerializer())
            producer.send(ProducerRecord(SOKNAD_TOPIC,"""
                {
                    "soknadId":"",
                    "sakId":"",
                    "soknad":""
                }
            """.trimIndent()))
            Thread.sleep(2000)
        }
    }
}