package no.nav.su.gsak

import com.ginsberg.junit.exit.ExpectSystemExit
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.github.tomakehurst.wiremock.matching.AnythingPattern
import io.ktor.http.HttpHeaders.Authorization
import io.ktor.http.HttpStatusCode.Companion.Created
import io.ktor.http.HttpStatusCode.Companion.ServiceUnavailable
import io.ktor.server.testing.withTestApplication
import io.ktor.util.KtorExperimentalAPI
import no.nav.common.KafkaEnvironment
import no.nav.su.gsak.EmbeddedKafka.Companion.embeddedKafka
import no.nav.su.gsak.EmbeddedKafka.Companion.testKafkaConsumer
import no.nav.su.meldinger.kafka.Topics.SØKNAD_TOPIC
import no.nav.su.meldinger.kafka.headersAsString
import no.nav.su.meldinger.kafka.soknad.NySøknad
import no.nav.su.meldinger.kafka.soknad.NySøknadMedSkyggesak
import no.nav.su.meldinger.kafka.soknad.SøknadMelding.Companion.fromConsumerRecord
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
    private val søknadId = "soknadId"
    private val aktørId = "aktoerId"
    private val correlationId = "correlationId"
    private val søknad = """{"key":"value"}"""
    private val fnr = "12345678910"
    private lateinit var embeddedKafka: KafkaEnvironment
    private lateinit var adminClient: AdminClient

    @Test
    fun `gitt at vi ikke har en skyggesak fra før av skal vi lage en ny skyggesak når vi får melding om ny sak`() {
        withTestApplication({
            testEnv(wireMockServer.baseUrl(), embeddedKafka.brokersURL)
            suGsak()
        }) {
            val kafkaConfig = KafkaConfigBuilder(environment.config)
            val producer = KafkaProducer(kafkaConfig.producerConfig(), StringSerializer(), StringSerializer())

            stubFor(noExistingGsak)
            stubFor(gsakCreated)

            producer.send(NySøknad(
                    sakId = sakId,
                    aktørId = aktørId,
                    søknadId = søknadId,
                    søknad = søknad,
                    fnr = fnr
            ).toProducerRecord(SØKNAD_TOPIC, mapOf(xCorrelationId to correlationId)))

            Thread.sleep(500)

            val records = testKafkaConsumer(embeddedKafka.brokersURL)
                    .poll(of(100, MILLIS))
                    .records(SØKNAD_TOPIC)

            verify(exactly(1), getRequestedFor(urlPathEqualTo("/rest/v1/sts/token")))
            verify(exactly(1), getRequestedFor(urlPathEqualTo("/saker")))
            verify(exactly(1), postRequestedFor(urlPathEqualTo("/saker")))

            assertEquals(2, records.count())
            assertTrue(fromConsumerRecord(records.first()) is NySøknad)
            assertEquals(correlationId, records.first().headersAsString()[xCorrelationId])
            assertTrue(fromConsumerRecord(records.last()) is NySøknadMedSkyggesak)
            assertEquals(correlationId, records.last().headersAsString()[xCorrelationId])

            val offsetMetadata = adminClient.listConsumerGroupOffsets(CONSUMER_GROUP_ID)
                    .partitionsToOffsetAndMetadata().get()
            assertEquals(2, offsetMetadata[offsetMetadata.keys.first()]?.offset())
        }
    }

    @Test
    @ExpectSystemExit
    fun `application should fail fast if exceptions are thrown`() {
        withTestApplication({
            testEnv(wireMockServer.baseUrl(), embeddedKafka.brokersURL)
            suGsak()
        }) {
            val kafkaConfig = KafkaConfigBuilder(environment.config)
            val producer = KafkaProducer(kafkaConfig.producerConfig(), StringSerializer(), StringSerializer())

            stubFor(gsakError)

            producer.send(NySøknad(
                    sakId = sakId,
                    aktørId = aktørId,
                    søknadId = søknadId,
                    søknad = søknad,
                    fnr = fnr
            ).toProducerRecord(SØKNAD_TOPIC, mapOf(xCorrelationId to correlationId)))

            Thread.sleep(500)

            verify(exactly(1), getRequestedFor(urlPathEqualTo("/rest/v1/sts/token")))
            verify(exactly(1), getRequestedFor(urlPathEqualTo("/saker")))

            val offsetMetadata = adminClient.listConsumerGroupOffsets(CONSUMER_GROUP_ID)
                    .partitionsToOffsetAndMetadata().get()
            assertEquals(0, offsetMetadata.keys.size) //Expect no offset committed
        }
    }

    private val gsakError = get(urlPathEqualTo("/saker"))
            .willReturn(aResponse().withStatus(ServiceUnavailable.value))

    private val noExistingGsak = get(urlPathEqualTo("/saker"))
            .withQueryParam("aktoerId", equalTo(aktørId))
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
                            "aktoerId":"$aktørId",
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
                            "aktoerId":"$aktørId",
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
    fun beforeEach() {
        embeddedKafka = embeddedKafka()
        adminClient = embeddedKafka.adminClient!!
        configureFor(wireMockServer.port())
        wireMockServer.resetAll()
        stubSts()
    }

    @AfterEach
    fun afterEach() {
        adminClient.close()
        embeddedKafka.tearDown()
    }
}