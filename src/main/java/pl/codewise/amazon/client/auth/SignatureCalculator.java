package pl.codewise.amazon.client.auth;

import io.netty.handler.codec.http.HttpHeaders;

public interface SignatureCalculator {

    void calculateAndAddSignature(HttpHeaders httpHeaders, String objectName, String contentMd5, String contentType, String virtualHost);
}
