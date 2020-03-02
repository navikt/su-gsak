package no.nav.su.gsak

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.github.tomakehurst.wiremock.matching.AnythingPattern
import io.ktor.http.HttpHeaders.Authorization
import io.ktor.http.HttpStatusCode.Companion.Created
import io.ktor.server.testing.withTestApplication
import io.ktor.util.KtorExperimentalAPI
import no.nav.common.KafkaEnvironment
import no.nav.su.gsak.EmbeddedKafka.Companion.kafkaConsumer
import no.nav.su.gsak.EmbeddedKafka.Companion.kafkaInstance
import no.nav.su.gsak.KafkaConfigBuilder.Topics.SOKNAD_TOPIC
import no.nav.su.meldinger.kafka.MessageBuilder.Companion.compatible
import no.nav.su.meldinger.kafka.MessageBuilder.Companion.toProducerRecord
import no.nav.su.meldinger.kafka.headersAsString
import no.nav.su.meldinger.kafka.soknad.NySoknad
import no.nav.su.meldinger.kafka.soknad.NySoknadHentGsak
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.*
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
    private lateinit var kafkaInstance: KafkaEnvironment
    private lateinit var adminClient: AdminClient


    @Test
    fun `gitt at vi ikke har en skyggesak fra før av skal vi lage en ny skyggesak når vi får melding om ny sak`() {
        withTestApplication({
            testEnv(wireMockServer.baseUrl(), kafkaInstance.brokersURL)
            sugsak()
        }) {
            val kafkaConfig = KafkaConfigBuilder(environment.config)
            val producer = KafkaProducer(kafkaConfig.producerConfig(), StringSerializer(), StringSerializer())

            stubFor(noExistingGsak)
            stubFor(gsakCreated)

            producer.send(toProducerRecord(SOKNAD_TOPIC, NySoknad(
                    sakId = sakId,
                    aktoerId = aktoerId,
                    soknadId = soknadId,
                    soknad = """{}"""
            ), mapOf(xCorrelationId to correlationId)))

            Thread.sleep(500)

            val records = kafkaConsumer(kafkaInstance.brokersURL)
                    .poll(of(100, MILLIS))
                    .records(SOKNAD_TOPIC)

            verify(exactly(1), getRequestedFor(urlPathEqualTo("/rest/v1/sts/token")))
            verify(exactly(1), getRequestedFor(urlPathEqualTo("/saker")))
            verify(exactly(1), postRequestedFor(urlPathEqualTo("/saker")))

            assertEquals(2, records.count())
            assertTrue(compatible(records.first(), NySoknad::class.java))
            assertEquals(correlationId, records.first().headersAsString()[xCorrelationId])
            assertTrue(compatible(records.last(), NySoknadHentGsak::class.java))
            assertEquals(correlationId, records.last().headersAsString()[xCorrelationId])

            val offsetMetadata = adminClient.listConsumerGroupOffsets(CONSUMER_GROUP_ID)
                    .partitionsToOffsetAndMetadata().get()

            assertEquals(2, offsetMetadata[offsetMetadata.keys.first()]?.offset())
        }
    }

    private val noExistingGsak = get(urlPathEqualTo("/saker"))
            .withQueryParam("aktoerId", equalTo(aktoerId))
            .withQueryParam("applikasjon", equalTo("SU-GSAK"))
            .withQueryParam("tema", equalTo("SU"))
            .withQueryParam("fagsakNr", equalTo(sakId))
            .withHeader(xCorrelationId, equalTo(correlationId))
            .withHeader(Authorization, equalTo("Bearer $STS_TOKEN"))
            .willReturn(okJson("[]"))


    private val gsakCreated = post(urlPathEqualTo("/saker"))
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
        kafkaInstance = kafkaInstance()
        adminClient = kafkaInstance.adminClient!!
        configureFor(wireMockServer.port())
        wireMockServer.resetAll()
        stubSts()
    }

    @AfterEach
    fun afterEach() {
        kafkaInstance.tearDown()
    }
}