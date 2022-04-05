package com.intuit.opensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

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


        //2. Create Kafka Consumer Clien
        //3. Subscribe the consumer to topic
        //4. Main Code Logic
        //5. Close the things
        openSearchClient.close();
    }
}
