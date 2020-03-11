package no.nav.su.gsak

import com.github.kittinunf.fuel.core.extensions.authentication
import com.github.kittinunf.fuel.core.extensions.jsonBody
import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.fuel.httpPost
import io.ktor.http.ContentType.Application.Json
import io.ktor.http.HttpHeaders.Accept
import no.nav.su.meldinger.kafka.soknad.NySøknad
import no.nav.su.person.sts.StsConsumer
import org.json.JSONArray
import org.json.JSONObject

internal class GsakConsumer(
        private val baseUrl: String,
        private val stsConsumer: StsConsumer
) {
    fun hentGsak(message: NySøknad) = hentGsak(message.sakId, message.aktørId, message.correlationId)
    private fun hentGsak(sakId: String, aktørId: String, correlationId: String): String {
        val (_, _, result) = "$baseUrl/saker".httpGet(listOf(
                "aktoerId" to aktørId,
                "applikasjon" to "SU-GSAK",
                "tema" to "SU",
                "fagsakNr" to sakId
        ))
                .authentication().bearer(stsConsumer.token())
                .header(Accept, Json)
                .header(xCorrelationId, correlationId)
                .responseString()

        return result.fold(
                { json ->
                    JSONArray(json).let {
                        when (it.isEmpty) {
                            true -> opprettGsak(sakId, aktørId, correlationId)
                            else -> JSONObject(it.first()).getString("id")
                        }
                    }
                },
                { throw GsakConsumerException("Could not get resource in GSAK for fagsak: $sakId, aktoer: $aktørId, $xCorrelationId:$correlationId, error: $it") }
        )
    }

    private fun opprettGsak(sakId: String, aktørId: String, correlationId: String): String {
        val (_, _, result) = "$baseUrl/saker".httpPost()
                .jsonBody("""
                    {
                        "tema":"SU",
                        "applikasjon":"SU-GSAK",
                        "aktoerId":"$aktørId",
                        "fagsakNr":"$sakId"
                    }
                """.trimIndent())
                .authentication().bearer(stsConsumer.token())
                .header(Accept, Json)
                .header(xCorrelationId, correlationId)
                .responseString()

        return result.fold(
                { JSONObject(it).getString("id") },
                { throw GsakConsumerException("Could not create resource in GSAK for fagsak: $sakId, aktoer: $aktørId, $xCorrelationId:$correlationId, error: $it") }
        )
    }
}

internal class GsakConsumerException(message: String) : java.lang.RuntimeException(message)