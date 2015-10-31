package pl.codewise.amazon.client.auth;

import io.netty.handler.codec.http.HttpHeaders;
import pl.codewise.amazon.client.http.Request;

public interface SignatureCalculator {

    void calculateAndAddSignature(HttpHeaders headers, Request requestData);
}
