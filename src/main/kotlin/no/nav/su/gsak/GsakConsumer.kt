package no.nav.su.gsak

import com.github.kittinunf.fuel.core.extensions.authentication
import com.github.kittinunf.fuel.core.extensions.jsonBody
import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.fuel.httpPost
import io.ktor.http.ContentType.Application.Json
import io.ktor.http.HttpHeaders.Accept
import no.nav.su.person.sts.StsConsumer
import org.json.JSONArray
import org.json.JSONObject
import java.util.*

internal class GsakConsumer(
        private val baseUrl: String,
        private val stsConsumer: StsConsumer
) {
    fun hentGsak(sakId: String, aktoerId: String): String {
        val (_, _, result) = "$baseUrl/saker".httpGet(listOf(
                "aktoerId" to aktoerId,
                "applikasjon" to "SU-GSAK",
                "tema" to "SU",
                "fagsakNr" to sakId
        ))
                .authentication().bearer(stsConsumer.token())
                .header(Accept, Json)
                .header("X-Correlation-ID", UUID.randomUUID())
                .responseString()

        return result.fold(
                { json ->
                    JSONArray(json).let {
                        when (it.isEmpty) {
                            true -> opprettGsak(sakId, aktoerId)
                            else -> JSONObject(it.first()).getString("id")
                        }
                    }
                },
                { opprettGsak(sakId, aktoerId) }
        )
    }

    private fun opprettGsak(sakId: String, aktoerId: String): String {
        val (_, _, result) = "$baseUrl/saker".httpPost()
                .jsonBody("""
                    {
                        "tema":"SU",
                        "applikasjon":"SU-GSAK",
                        "aktoerId":"$aktoerId",
                        "fagsakNr":"$sakId"
                    }
                """.trimIndent())
                .authentication().bearer(stsConsumer.token())
                .header(Accept, Json)
                .header("X-Correlation-ID", UUID.randomUUID())
                .responseString()

        return result.fold(
                { JSONObject(it).getString("id") },
                { throw RuntimeException("Could not create resource in GSAK sak for fagsak: $sakId, aktoer: $aktoerId, message: ${it.errorData}") }
        )
    }
}