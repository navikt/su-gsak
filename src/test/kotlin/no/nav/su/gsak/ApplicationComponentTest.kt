package no.nav.su.gsak

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.github.tomakehurst.wiremock.matching.AnythingPattern
import io.ktor.http.HttpHeaders.Authorization
import io.ktor.http.HttpStatusCode.Companion.Created
import io.ktor.server.testing.withTestApplication
import io.ktor.util.KtorExperimentalAPI
import no.nav.su.gsak.KafkaConfigBuilder.Topics.SOKNAD_TOPIC
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.json.JSONObject
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration.of
import java.time.temporal.ChronoUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


@KtorExperimentalAPI
class ApplicationComponentTest {

    private val sakId = "sakId"
    private val soknadId = "soknadId"
    private val aktoerId = "aktoerId"

    @Test
    fun `gitt at vi ikke har en skyggesak fra før av skal vi lage en ny skyggesak når vi får melding om ny sak`() {
        withTestApplication({
            testEnv(wireMockServer.baseUrl())
            sugsak()
        }) {
            val kafkaConfig = KafkaConfigBuilder(environment.config)
            val producer = KafkaProducer(kafkaConfig.producerConfig(), StringSerializer(), StringSerializer())

            stubFor(get(urlPathEqualTo("/saker"))
                    .withQueryParam("aktoerId", equalTo(aktoerId))
                    .withQueryParam("applikasjon", equalTo("SU-GSAK"))
                    .withQueryParam("tema", equalTo("SU"))
                    .withQueryParam("fagsakNr", equalTo(sakId))
                    .withHeader("X-Correlation-ID", AnythingPattern())
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
                    .withHeader("X-Correlation-ID", AnythingPattern())
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

            producer.send(ProducerRecord(SOKNAD_TOPIC, """
                {
                    "aktoerId":"$aktoerId",
                    "soknadId":"$soknadId",
                    "sakId":"$sakId",
                    "soknad": {}
                }
            """.trimIndent()))

            Thread.sleep(2000)

            verify(exactly(1), getRequestedFor(urlPathEqualTo("/rest/v1/sts/token")))
            verify(exactly(1), getRequestedFor(urlPathEqualTo("/saker")))
            verify(exactly(1), postRequestedFor(urlPathEqualTo("/saker")))

            val records = EmbeddedKafka.kafkaConsumer.poll(of(100, ChronoUnit.MILLIS)).records(SOKNAD_TOPIC)
            assertEquals(2, records.count())
            val consumed = JSONObject(records.first().value())
            assertFalse(consumed.has("gsakId"))
            val produced = JSONObject(records.last().value())
            assertTrue(produced.has("gsakId"))
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