package no.ssb.sagalog.pulsar;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class PulsarSagaLogIdTest {

    @Test
    public void thatGetSimpleLogNameWorks() {
        PulsarSagaLogId logIdFromParts = new PulsarSagaLogId("mycompany", "internal-sagalog-integration-testing", "01", "hi");
        PulsarSagaLogId logIdFromPath = new PulsarSagaLogId(logIdFromParts.getTopic());
        assertEquals(logIdFromPath, logIdFromParts);
    }

    @Test
    public void thatGetAdvancedLogNameWorks() {
        PulsarSagaLogId logIdFromParts = new PulsarSagaLogId("mycompany", "internal-sagalog-integration-testing", "01", "hola-.:$there");
        PulsarSagaLogId logIdFromPath = new PulsarSagaLogId(logIdFromParts.getTopic());
        assertEquals(logIdFromPath, logIdFromParts);
    }
}
