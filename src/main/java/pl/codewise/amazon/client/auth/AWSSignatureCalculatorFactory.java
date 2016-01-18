package pl.codewise.amazon.client.auth;

import com.amazonaws.auth.AWSCredentialsProvider;
import io.netty.util.concurrent.FastThreadLocal;

public class AWSSignatureCalculatorFactory {

    private final FastThreadLocal<AWSSignatureCalculator> signatureCalculator;

    public AWSSignatureCalculatorFactory(AWSCredentialsProvider credentialsProvider) {
        signatureCalculator = new FastThreadLocal<AWSSignatureCalculator>() {
            @Override
            protected AWSSignatureCalculator initialValue() {
                return new AWSSignatureCalculator(credentialsProvider);
            }
        };
    }

    public AWSSignatureCalculator getSignatureCalculator() {
        return signatureCalculator.get();
    }
}
