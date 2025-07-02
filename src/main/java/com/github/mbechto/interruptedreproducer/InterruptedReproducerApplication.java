package com.github.mbechto.interruptedreproducer;

import static org.axonframework.modelling.command.AggregateCreationPolicy.CREATE_IF_MISSING;
import static org.axonframework.modelling.command.AggregateLifecycle.apply;

import java.time.Duration;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.config.ConfigurerModule;
import org.axonframework.config.EventProcessingConfiguration;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.gateway.EventGateway;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.deadletter.InMemorySequencedDeadLetterQueue;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.modelling.command.CreationPolicy;
import org.axonframework.modelling.command.TargetAggregateIdentifier;
import org.axonframework.spring.stereotype.Aggregate;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class InterruptedReproducerApplication {

  private static final boolean REPRODUCE_BUG = true;

  public static void main(String[] args) throws InterruptedException {
    var ctx = SpringApplication.run(InterruptedReproducerApplication.class, args);
    var eventGateway = ctx.getBean(EventGateway.class);
    eventGateway.publish(new MyEvent("0", true), new MyEvent("1", false));
    var config = ctx.getBean(EventProcessingConfiguration.class);

    long size = 0;
    while (!Thread.currentThread().isInterrupted()) {
      long newSize = config.deadLetterQueue("mypg").orElseThrow().amountOfSequences();
      if (newSize != size) {
        size = newSize;
        System.out.println("--> dlq size: " + size);
      }
      Thread.sleep(2000);
    }
  }

  @Component
  @ProcessingGroup("mypg")
  public static class MyProcessor {
    @EventHandler
    public void handle(MyEvent event, CommandGateway commandGateway) {
      commandGateway.sendAndWait(new MyCommand(event.id, event.shouldFail));
    }
  }

  public record MyCommand(@TargetAggregateIdentifier String id, boolean shouldFail) {}

  public record MyEvent(String id, boolean shouldFail) {}

  public record MyAggregateCreated(String id) {}

  @Aggregate
  public static class MyAggregate {
    @AggregateIdentifier private String id;

    MyAggregate() {}

    @CommandHandler
    @CreationPolicy(CREATE_IF_MISSING)
    public void handle(MyCommand cmd) throws InterruptedException {
      RetryTemplate retryTemplate =
          RetryTemplate.builder()
              /* To reproduce the issue, provoke an axon-janitor triggered interrupt while Spring Retry is in backoff. */
              .maxAttempts(REPRODUCE_BUG ? 2 : 1)
              .fixedBackoff(Duration.ofMinutes(1)) // must be greater than the handling timeout
              .retryOn(IllegalStateException.class)
              .build();

      retryTemplate.execute(
          ctx -> {
            System.out.println("--> " + cmd);

            if (cmd.shouldFail()) {
              if (!REPRODUCE_BUG) {
                Thread.sleep(60000); // throws InterruptedException when axon-janitor interrupts
              } else {
                // causes a BackOffInterruptedException (which is not an InterruptedException)
                throw new IllegalStateException("Error to trigger a retry backoff");
              }
            }

            return apply(new MyAggregateCreated(cmd.id));
          });
    }

    @EventSourcingHandler
    public void handle(MyAggregateCreated event) {
      this.id = event.id;
      System.out.println("--> " + event);
    }
  }

  @Bean
  public EventStorageEngine inMemoryEventStorageEngine() {
    return new InMemoryEventStorageEngine();
  }

  @Bean
  public ConfigurerModule inMemoryDlq() {
    return configurer ->
        configurer
            .eventProcessing()
            .registerDeadLetterQueueProvider(
                pg ->
                    config -> InMemorySequencedDeadLetterQueue.<EventMessage<?>>builder().build());
  }
}
