package no.nav.su.gsak

import com.github.kittinunf.fuel.core.extensions.authentication
import com.github.kittinunf.fuel.httpGet
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import no.nav.su.person.sts.StsConsumer

class GsakConsumer(
    private val baseUrl: String,
    private val stsConsumer: StsConsumer
) {
    fun hentGsak(sakId: String) {
        val (_, _, result) = baseUrl.httpGet()
            .authentication().bearer(stsConsumer.token())
            .header(HttpHeaders.Accept, ContentType.Application.Json)
            .header("X-Correlation-ID",)
            .responseString()
        result.fold(
            {},
            {}
        )
    }
}