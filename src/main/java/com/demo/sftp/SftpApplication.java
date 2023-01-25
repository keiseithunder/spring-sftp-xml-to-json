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
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.remote.gateway.AbstractRemoteFileOutboundGateway.Command;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.integration.sftp.filters.SftpSimplePatternFileListFilter;
import org.springframework.integration.sftp.gateway.SftpOutboundGateway;
import org.springframework.integration.sftp.inbound.SftpStreamingMessageSource;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.integration.transformer.HeaderEnricher;
import org.springframework.integration.transformer.StreamTransformer;
import org.springframework.integration.transformer.support.ExpressionEvaluatingHeaderValueMessageProcessor;
import org.springframework.integration.transformer.support.HeaderValueMessageProcessor;
import org.springframework.integration.xml.transformer.UnmarshallingTransformer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import com.demo.sftp.models.Note;

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
  public MessageChannel streamChannel() {
    return new PublishSubscribeChannel();
  }

  @Bean
  @InboundChannelAdapter(channel = "streamChannel", poller = @Poller(fixedDelay = "10000"))
  public MessageSource<InputStream> sftpMessageSource() {
    SftpStreamingMessageSource messageSource = new SftpStreamingMessageSource(template());
    messageSource.setRemoteDirectory(remoteDir);
    messageSource.setFilter(new SftpSimplePatternFileListFilter("*.xml"));
    messageSource.setMaxFetchSize(-1);
    messageSource.setHeaderExpressions(Collections.singletonMap(MessageHeaders.ERROR_CHANNEL,
        new LiteralExpression(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME)));
    return messageSource;
  }

  @Bean
  @Transformer(inputChannel = "streamChannel", outputChannel = "rawdata")
  public org.springframework.integration.transformer.Transformer transformer() {
    return new StreamTransformer("UTF-8");
  }

  @Bean
  @Order(Ordered.LOWEST_PRECEDENCE)
  @ServiceActivator(inputChannel = "streamChannel")
  public MessageHandler moveFile() {
    SftpOutboundGateway sftpOutboundGateway = new SftpOutboundGateway(sftpSessionFactory(), Command.MV.getCommand(),
        "headers['file_remoteDirectory'] + '/' + headers['file_remoteFile']");
    sftpOutboundGateway
        .setRenameExpressionString(
            "headers['file_remoteDirectory'] + '/processed/' +headers['timestamp'] + '-' + headers['file_remoteFile']");
    sftpOutboundGateway.setRequiresReply(false);
    sftpOutboundGateway.setUseTemporaryFileName(true);
    sftpOutboundGateway.setOutputChannelName("nullChannel");
    sftpOutboundGateway.setOrder(Ordered.LOWEST_PRECEDENCE);
    sftpOutboundGateway.setAsync(true);
    return sftpOutboundGateway;
  }

  @Bean
  @Order(Ordered.LOWEST_PRECEDENCE)
  @ServiceActivator(inputChannel = IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME)
  public MessageHandler moveErrorFile() {
    SftpOutboundGateway sftpOutboundGateway = new SftpOutboundGateway(sftpSessionFactory(), Command.MV.getCommand(),
        "payload['failedMessage']['headers']['file_remoteDirectory'] + '/' + payload['failedMessage']['headers']['file_remoteFile']");
    sftpOutboundGateway
        .setRenameExpressionString(
            "payload['failedMessage']['headers']['file_remoteDirectory'] + '/error/' + payload['failedMessage']['headers']['timestamp'] + '-' + payload['failedMessage']['headers']['file_remoteFile']");
    sftpOutboundGateway.setRequiresReply(false);
    sftpOutboundGateway.setUseTemporaryFileName(true);
    sftpOutboundGateway.setOutputChannelName("nullChannel");
    sftpOutboundGateway.setOrder(Ordered.HIGHEST_PRECEDENCE);
    sftpOutboundGateway.setAsync(true);
    return sftpOutboundGateway;
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
    Expression kafkaKey = new SpelExpressionParser().parseExpression("headers['file_remoteFile']");
    Expression processedRenamePath = new SpelExpressionParser()
        .parseExpression("headers['file_remoteDirectory'] + '/processed/' + headers['file_remoteFile']");
    headersToAdd.put(KafkaHeaders.MESSAGE_KEY,
        new ExpressionEvaluatingHeaderValueMessageProcessor<>(kafkaKey, String.class));
    headersToAdd.put(FileHeaders.RENAME_TO,
        new ExpressionEvaluatingHeaderValueMessageProcessor<>(processedRenamePath, String.class));
    return new HeaderEnricher(headersToAdd);
  }

  @Bean
  @ServiceActivator(inputChannel = "toKafka2")
  public MessageHandler handlerKafka() throws Exception {
    KafkaProducerMessageHandler<String, String> handler = new KafkaProducerMessageHandler<>(kafkaTemplate());
    handler.setTopicExpression(new LiteralExpression("test"));
    handler.setSendSuccessChannelName("success");
    return handler;
  }

  @Bean
  @ServiceActivator(inputChannel = "success")
  public MessageHandler successHandler() {
    return System.out::println;
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

}
