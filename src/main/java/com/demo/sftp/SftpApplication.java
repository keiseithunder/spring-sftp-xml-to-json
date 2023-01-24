package com.demo.sftp;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.sshd.sftp.client.SftpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.annotation.Filter;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.filters.AcceptAllFileListFilter;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.handler.advice.ExpressionEvaluatingRequestHandlerAdvice;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.integration.sftp.gateway.SftpOutboundGateway;
import org.springframework.integration.sftp.inbound.SftpStreamingMessageSource;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.integration.transformer.HeaderEnricher;
import org.springframework.integration.transformer.StreamTransformer;
import org.springframework.integration.transformer.support.ExpressionEvaluatingHeaderValueMessageProcessor;
import org.springframework.integration.transformer.support.HeaderValueMessageProcessor;
import org.springframework.integration.transformer.support.StaticHeaderValueMessageProcessor;
import org.springframework.integration.xml.transformer.UnmarshallingTransformer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import com.demo.sftp.models.Note;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
public class SftpApplication {

  @Value("${sftp.host}")
  private String host;
  @Value("${sftp.port}")
  private int port;
  @Value("${sftp.username}")
  private String user;
  @Value("${sftp.password}")
  private String password;
  @Value("${sftp.remote.directory}")
  private String remoteDir;

  public static void main(String[] args) {
    SpringApplication.run(SftpApplication.class, args);
  }

  @Bean
  public SessionFactory<SftpClient.DirEntry> sftpSessionFactory() {
    DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory(true);
    factory.setHost(host);
    factory.setPort(port);
    factory.setUser(user);
    factory.setPassword(password);
    factory.setAllowUnknownKeys(true);
    return new CachingSessionFactory<>(factory);
  }

  @Bean
  public SftpRemoteFileTemplate template() {
    return new SftpRemoteFileTemplate(sftpSessionFactory());
  }

  @Bean
  @InboundChannelAdapter(channel = "stream", poller =  @Poller(fixedDelay = "10000"))
  public MessageSource<InputStream> ftpMessageSource() {
    SftpStreamingMessageSource messageSource = new SftpStreamingMessageSource(template());
    messageSource.setRemoteDirectory(remoteDir);
    messageSource.setFilter(new AcceptAllFileListFilter<>());
    messageSource.setMaxFetchSize(-1);
    return messageSource;
  }

  @Bean
  @Transformer(inputChannel = "stream", outputChannel = "rawdata")
  public org.springframework.integration.transformer.Transformer transformer() {
    return new StreamTransformer("UTF-8");
  }

  @Bean
  @Transformer(inputChannel = "rawdata", outputChannel = "toKafka")
  public UnmarshallingTransformer marshallingTransformer() {
    Jaxb2Marshaller jaxb2Marshaller = new Jaxb2Marshaller();
    jaxb2Marshaller.setClassesToBeBound(Note.class);

    return new UnmarshallingTransformer(jaxb2Marshaller);
  }

  @Bean
  @Transformer(inputChannel = "toKafka", outputChannel = "toKafka2")
  public HeaderEnricher headerEnricher() {
    Map<String, HeaderValueMessageProcessor<?>> headersToAdd = new HashMap<>();
    Expression ex = new SpelExpressionParser().parseExpression("headers['file_remoteFile']");
    headersToAdd.put(KafkaHeaders.MESSAGE_KEY, new ExpressionEvaluatingHeaderValueMessageProcessor<>(ex, String.class));
    return new HeaderEnricher(headersToAdd);
  }

  @Bean
  @ServiceActivator(inputChannel = "toKafka2")
  public MessageHandler handlerKafka() throws Exception {
    KafkaProducerMessageHandler<String, String> handler = new KafkaProducerMessageHandler<>(kafkaTemplate());
    handler.setTopicExpression(new LiteralExpression("test"));
    handler.setSendSuccessChannelName("success");
    // handler.setSendFailureChannelName("failure");
    return handler;
  }

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  @Bean
  public ProducerFactory<String, String> producerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    JsonSerializer jsonSerde = new JsonSerializer<>();
    return new DefaultKafkaProducerFactory<>(props, null, jsonSerde.copyWithType(Note.class));
  }



  @Bean
  @ServiceActivator(inputChannel = "success", adviceChain = "after")
  public MessageHandler handle() {
    return System.out::println;
  }

  @Bean
  public ExpressionEvaluatingRequestHandlerAdvice after() {
    ExpressionEvaluatingRequestHandlerAdvice advice = new ExpressionEvaluatingRequestHandlerAdvice();
    advice.setOnSuccessExpressionString(
        "@template.remove(headers['file_remoteDirectory']+'/'+headers['file_remoteFile'])");
    advice.setPropagateEvaluationFailures(true);
    return advice;
  }



}
