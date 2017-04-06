package org.apache.ignite.stream.akka;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Created by kmax on 06.04.17.
 */
public class ExampleAkkaStreamToIgniteStream {
    public static void main(String[] args) {

        ActorSystem system = ActorSystem.create("Ignite");
        ActorMaterializer materializer = ActorMaterializer.create(system);

        IgniteAkkaStreamer igniteStreamer = new IgniteAkkaStreamer();

        // Starting from a Source
        final Source<Integer, NotUsed> source = Source.from(Arrays.asList(1, 2, 3, 4));

        final Flow<Integer, Integer, NotUsed> flow = Flow.of(Integer.class).map(elem -> elem + 20);

        RunnableGraph<NotUsed> r1 = source.via(flow).to(igniteStreamer.sink());

        r1.run(materializer);
    }
}
