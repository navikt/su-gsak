package no.nav.su.gsak

import com.github.tomakehurst.wiremock.client.WireMock.*
import io.ktor.application.Application
import io.ktor.config.MapApplicationConfig
import io.ktor.util.KtorExperimentalAPI

const val USERNAME = "srvUser"
const val PASSWORD = "srvPassword"
const val STS_TOKEN = "stsToken"

@KtorExperimentalAPI
fun Application.testEnv(wireMockUrl: String = "", brokersURL: String) {
    (environment.config as MapApplicationConfig).apply {
        put("sts.url", wireMockUrl)
        put("serviceuser.username", USERNAME)
        put("serviceuser.password", PASSWORD)
        put("kafka.username", USERNAME)
        put("kafka.password", PASSWORD)
        put("kafka.bootstrap", brokersURL)
        put("kafka.trustStorePath", "")
        put("kafka.trustStorePassword", "")
        put("gsak.url", wireMockUrl)
    }
}

fun stubSts() {
    stubFor(get(urlPathEqualTo("/rest/v1/sts/token"))
            .withQueryParam("grant_type", equalTo("client_credentials"))
            .withQueryParam("scope", equalTo("openid"))
            .withBasicAuth(USERNAME, PASSWORD)
            .withHeader("Accept", equalTo("application/json"))
            .willReturn(okJson("""
                {
                    "access_token": "$STS_TOKEN",
                    "token_type": "Bearer",
                    "expires_in": 3600
                }
            """.trimIndent())
            ))
}