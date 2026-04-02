package org.codeserengeti.mes.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MesFlinkJobTest {

    @Test
    void testProcessEventFunction() throws Exception {
        MesFlinkJob.ProcessEventFunction fn = new MesFlinkJob.ProcessEventFunction();
        assertEquals("[MES] test-event", fn.map("test-event"));
        assertEquals("[MES] ", fn.map(""));
    }

    @Test
    void testBuildPipelineDoesNotThrow() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // buildPipeline should not throw during graph construction
        MesFlinkJob.buildPipeline(env);
    }
}
