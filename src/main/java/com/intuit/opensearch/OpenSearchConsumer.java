package com.intuit.opensearch;



import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {



    public static RestHighLevelClient createOpenSearchClient() {
       String connString="https://m7852y50gz:e7e0v26rj4@kafka-es-cluster-5829139906.us-east-1.bonsaisearch.net:443";
        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    public static KafkaConsumer<String,String> createKafkaConsumer(){
        String bootstrapServer="localhost:9092";
        String groupId="opensearch-consumer-group";
        //Create Consumer Configs
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest"); // earliest/latest/none
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        //Create a Consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);

        return  consumer;
    }



    public static void main(String[] args) throws IOException {

        Logger logger= LoggerFactory.getLogger(OpenSearchConsumer.class.getName());
        //1. Create an OpenSearch Client

        RestHighLevelClient openSearchClient=createOpenSearchClient();

        // We need to create an index on OpenSearch if it doesn't exists

        try{

            boolean indexExists=openSearchClient.indices().exists(new GetIndexRequest("wikimedia"),RequestOptions.DEFAULT);
            if(!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("Index has been created");
            }
            else{
                logger.info("Index already exists");
            }
        }
        catch (Exception e){
            logger.error("Error in creating Index:",e);
        }


        //2. Create Kafka Consumer Client

        KafkaConsumer<String,String> consumer=createKafkaConsumer();
        //3. Subscribe the consumer to topic
        consumer.subscribe(Collections.singleton("wikimedia.changes"));
        //4. Main Code Logic

        while(true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(3000));

            int recordCount=records.count();
            logger.info("RECIEVED "+recordCount+" records(s)");



            for(ConsumerRecord<String,String> record:records){
                //Send the Record to OpenSearch
                // to make records unique for OpenSearch, we will have to assign key

                //Strategy 1
                String id=record.topic()+"_"+record.partition()+"_"+record.offset();

                //Stratergy 2



            try{


                IndexRequest indexRequest=new IndexRequest("wikimedia")
                        .source(record.value(), XContentType.JSON)
                        .id(id);

                IndexResponse response=openSearchClient.index(indexRequest,RequestOptions.DEFAULT);

               // logger.info("Inserted 1 document in OpenSearch:"+response.getId());
            }
            catch (Exception e){

            }
            }

            //Commit the offset after a batch is processed
            consumer.commitSync();
            logger.info("Offsets have been committed");
        }



        //5. Close the things
     //   openSearchClient.close();
    }
}
