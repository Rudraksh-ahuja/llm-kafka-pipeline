package com.pipeline;

import com.pipeline.service.LlmProcessorService;

public class KafkaLlmApplication {
    public static void main(String[] args) {
        LlmProcessorService pipelineService = new LlmProcessorService("input-prompts", "output-responses");

        // Register shutdown hook to handle elegant application context termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("System interrupt intercepted. Safely draining resources...");
        }));

        pipelineService.startProcessingLoop();
    }
}