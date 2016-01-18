package pl.codewise.amazon.client.utils;

import java.util.concurrent.atomic.AtomicReference;

import javolution.text.TextBuilder;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TextBuildersTest {

    @Test
    public void shouldReuseBuilders() {
        // Given
        TextBuilder expected = TextBuilders.threadLocal();

        // When
        TextBuilder actual = TextBuilders.threadLocal();

        // Then
        assertThat(actual).isSameAs(expected);
    }

    @Test
    public void shouldReturnDifferentBuildersForDifferentThreads() throws InterruptedException {
        // Given
        TextBuilder mainThreadBuilder = TextBuilders.threadLocal();
        AtomicReference<TextBuilder> otherThreadBuilder = new AtomicReference<>();

        Thread thread = new Thread(() -> otherThreadBuilder.set(TextBuilders.threadLocal()));

        // When
        thread.start();
        thread.join();

        // Then
        assertThat(otherThreadBuilder.get()).isNotNull();
        assertThat(mainThreadBuilder).isNotSameAs(otherThreadBuilder.get());
    }

    @Test
    public void shouldResetBuilder() {
        // Given
        TextBuilder builder = TextBuilders.threadLocal();
        builder.append("Test");

        // When
        builder = TextBuilders.threadLocal();

        // Then
        assertThat(builder.length()).isZero();
    }
}
