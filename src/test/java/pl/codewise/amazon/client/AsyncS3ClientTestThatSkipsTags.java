package pl.codewise.amazon.client;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

@Test(enabled = false)
public class AsyncS3ClientTestThatSkipsTags extends AsyncS3ClientTest {

    @BeforeClass
    public void setUpConfigurationThatDoesNotSkipTags() throws IOException {
        fieldsToIgnore.addAll(Arrays.asList("lastModified", "storageClass", "owner"));

        configuration = ClientConfiguration
                .builder()
                .skipParsingETag()
                .skipParsingLasModified()
                .skipParsingStorageClass()
                .skipParsingOwner()
                .useCredentials(credentials)
                .build();
    }
}
