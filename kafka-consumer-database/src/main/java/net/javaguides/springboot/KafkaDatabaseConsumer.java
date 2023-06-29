package net.javaguides.springboot;


import net.javaguides.springboot.entity.WikimediaData;
import net.javaguides.springboot.repository.WikimediaDataRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {
    private static final Logger LOGGER= LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    public KafkaDatabaseConsumer(WikimediaDataRepo dataRepo) {
        this.dataRepo = dataRepo;
    }

    private WikimediaDataRepo dataRepo;


    @KafkaListener(topics = "wikimedia_recentchanges",
                   groupId= "myGroup")
    public void consumer(String eventMessage){
        LOGGER.info(String.format("Event message received ->%s",eventMessage));


        WikimediaData wikimediaData=new WikimediaData();
        wikimediaData.setWikiEventData(eventMessage);

        dataRepo.save(wikimediaData);

    }
}
