package pl.codewise.amazon.client.auth;

import com.amazonaws.auth.AWSCredentialsProvider;

public class AWSSignatureCalculatorFactory {

    private final ThreadLocal<AWSSignatureCalculator> signatureCalculator;


    public AWSSignatureCalculatorFactory(AWSCredentialsProvider credentialsProvider) {
        signatureCalculator = ThreadLocal.withInitial(() -> new AWSSignatureCalculator(credentialsProvider));
    }

    public AWSSignatureCalculator getSignatureCalculator() {
        return signatureCalculator.get();
    }
}
