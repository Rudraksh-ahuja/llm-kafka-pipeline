package com.pipeline.service;

import com.pipeline.config.KafkaClusterConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.ollama.OllamaChatModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LlmProcessorService {

    private static final Logger log = LoggerFactory.getLogger(LlmProcessorService.class);
    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final ChatLanguageModel llmClient;
    private final String inputTopic;
    private final String outputTopic;

    public LlmProcessorService(String inputTopic, String outputTopic) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.consumer = new KafkaConsumer<>(KafkaClusterConfig.getConsumerProperties("llm-processor-group"));
        this.producer = new KafkaProducer<>(KafkaClusterConfig.getProducerProperties("tx-llm-pipeline-id"));

        // Initialize LangChain4j and bind it to the local Ollama instance
        this.llmClient = OllamaChatModel.builder()
                .baseUrl("http://localhost:11434")
                .modelName("llama3")
                .temperature(0.7)
                .build();
    }

    public void startProcessingLoop() {
        // Step 1: Initialize transactions with the Kafka coordinator
        producer.initTransactions();
        consumer.subscribe(Collections.singletonList(inputTopic));
        log.info("Production Transactional Loop Engine Started successfully.");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) continue;

                // Step 2: Begin Transaction Block for Exactly-Once Semantics (EOS)
                try {
                    producer.beginTransaction();

                    for (ConsumerRecord<String, String> record : records) {
                        log.info("Processing inbound prompt record from partition {}: {}", record.partition(), record.value());

                        // Execute inference through LangChain4j core
                        String systemContextPrompt = "You are an enterprise stream analyst. Summarize this request context cleanly in one sentence: " + record.value();
                        String llmResponse = llmClient.generate(systemContextPrompt);

                        ProducerRecord<String, String> outRecord = new ProducerRecord<>(outputTopic, record.key(), llmResponse);
                        producer.send(outRecord);
                    }

                    // Step 3: Calculate and stage offsets inside the active transaction
                    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                    for (TopicPartition partition : records.partitions()) {
                        long lastOffset = records.records(partition).get(records.records(partition).size() - 1).offset();
                        offsetsToCommit.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }

                    producer.sendOffsetsToTransaction(offsetsToCommit, consumer.groupMetadata());

                    // Step 4: Commit both messages and read-offsets atomically
                    producer.commitTransaction();
                    log.info("Batch transaction committed successfully.");

                } catch (ProducerFencedException e) {
                    log.error("Fatal: Zombie-node detected. Producer has been fenced out.", e);
                    producer.close();
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    log.error("Exception intercepted during processing loop runtime. Aborting current transaction...", e);
                    producer.abortTransaction();
                }
            }
        } finally {
            consumer.close();
            producer.close();
        }
    }
}