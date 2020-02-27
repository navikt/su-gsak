package no.nav.su.gsak

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.github.tomakehurst.wiremock.matching.AnythingPattern
import io.ktor.http.HttpHeaders.Authorization
import io.ktor.http.HttpStatusCode.Companion.Created
import io.ktor.server.testing.withTestApplication
import io.ktor.util.KtorExperimentalAPI
import no.nav.su.gsak.EmbeddedKafka.Companion.kafkaConsumer
import no.nav.su.gsak.KafkaConfigBuilder.Topics.SOKNAD_TOPIC
import no.nav.su.meldinger.kafka.MessageBuilder.Companion.compatible
import no.nav.su.meldinger.kafka.MessageBuilder.Companion.toProducerRecord
import no.nav.su.meldinger.kafka.headersAsString
import no.nav.su.meldinger.kafka.soknad.NySoknad
import no.nav.su.meldinger.kafka.soknad.NySoknadHentGsak
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration.of
import java.time.temporal.ChronoUnit.MILLIS
import kotlin.test.assertEquals
import kotlin.test.assertTrue


@KtorExperimentalAPI
class ApplicationComponentTest {

    private val sakId = "sakId"
    private val soknadId = "soknadId"
    private val aktoerId = "aktoerId"
    private val correlationId = "abcdef"

    @Test
    fun `gitt at vi ikke har en skyggesak fra før av skal vi lage en ny skyggesak når vi får melding om ny sak`() {
        withTestApplication({
            testEnv(wireMockServer.baseUrl())
            sugsak()
        }) {
            val kafkaConfig = KafkaConfigBuilder(environment.config)
            val producer = KafkaProducer(kafkaConfig.producerConfig(), StringSerializer(), StringSerializer())
            val adminClient = EmbeddedKafka.kafkaInstance.adminClient!!
            stubFor(get(urlPathEqualTo("/saker"))
                    .withQueryParam("aktoerId", equalTo(aktoerId))
                    .withQueryParam("applikasjon", equalTo("SU-GSAK"))
                    .withQueryParam("tema", equalTo("SU"))
                    .withQueryParam("fagsakNr", equalTo(sakId))
                    .withHeader(xCorrelationId, equalTo(correlationId))
                    .withHeader(Authorization, equalTo("Bearer $STS_TOKEN"))
                    .willReturn(okJson("[]"))
            )

            stubFor(post(urlPathEqualTo("/saker"))
                    .withRequestBody(equalToJson("""
                        {
                            "tema":"SU",
                            "applikasjon":"SU-GSAK",
                            "aktoerId":"$aktoerId",
                            "fagsakNr":"$sakId"
                        }
                    """.trimIndent()))
                    .withHeader(xCorrelationId, AnythingPattern())
                    .withHeader(Authorization, equalTo("Bearer $STS_TOKEN"))
                    .willReturn(aResponse()
                            .withStatus(Created.value)
                            .withBody("""
                        {
                            "id":"1",
                            "tema":"SU",
                            "applikasjon":"SU-GSAK",
                            "aktoerId":"$aktoerId",
                            "fagsakNr":"$sakId"
                        }
                    """.trimIndent()))
            )

            producer.send(toProducerRecord(SOKNAD_TOPIC, NySoknad(
                    sakId = sakId,
                    aktoerId = aktoerId,
                    soknadId = soknadId,
                    soknad = """{}"""
            ), mapOf(xCorrelationId to correlationId)))

            Thread.sleep(2000)

            verify(exactly(1), getRequestedFor(urlPathEqualTo("/rest/v1/sts/token")))
            verify(exactly(1), getRequestedFor(urlPathEqualTo("/saker")))
            verify(exactly(1), postRequestedFor(urlPathEqualTo("/saker")))

            val records = kafkaConsumer.poll(of(100, MILLIS)).records(SOKNAD_TOPIC)
            assertEquals(2, records.count())
            assertTrue(compatible(records.first(), NySoknad::class.java))
            assertEquals("abcdef", records.first().headersAsString()[xCorrelationId])
            assertTrue(compatible(records.last(), NySoknadHentGsak::class.java))
            assertEquals("abcdef", records.last().headersAsString()[xCorrelationId])

//            Depends on commit interval
//            val offsetMetadata = adminClient.listConsumerGroupOffsets("su-gsak").partitionsToOffsetAndMetadata().get()
//            assertEquals(2, offsetMetadata[offsetMetadata.keys.first()]?.offset())
        }
    }

    companion object {
        val wireMockServer: WireMockServer = WireMockServer(WireMockConfiguration.options().dynamicPort())

        @BeforeAll
        @JvmStatic
        fun beforeAll() {
            wireMockServer.start()
        }

        @AfterAll
        @JvmStatic
        fun afterAll() {
            wireMockServer.stop()
        }
    }

    @BeforeEach
    fun configure() {
        configureFor(wireMockServer.port())
        stubSts()
    }
}