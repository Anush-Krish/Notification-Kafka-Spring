package net.javaguides.springboot;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

@Service
public class WikimediaChangesProducer {
  private static final Logger LOGGER= (Logger) LoggerFactory.getLogger(WikimediaChangesProducer.class);

  public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  private KafkaTemplate<String,String> kafkaTemplate;

  public void sendMessage() throws InterruptedException{
    String topic="wikimedia_recentchanges";
    //to read realtime data from wikimedia.
    EventHandler EventHandler=new WikimediaChangesHandler(kafkaTemplate,topic);
    String url="https://stream.wikimedia.org/v2/stream/recentchange";
    EventSource.Builder builder= new EventSource.Builder(EventHandler,URI.create(url));
    EventSource eventSource= builder.build();
    eventSource.start();

    TimeUnit.MINUTES.sleep(10);
  }

}
