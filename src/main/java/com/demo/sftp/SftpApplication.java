package com.demo.sftp;

import java.io.InputStream;

import org.apache.sshd.sftp.client.SftpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.filters.AcceptAllFileListFilter;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.handler.advice.ExpressionEvaluatingRequestHandlerAdvice;
import org.springframework.integration.sftp.inbound.SftpStreamingMessageSource;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.integration.transformer.StreamTransformer;
import org.springframework.integration.xml.transformer.UnmarshallingTransformer;
import org.springframework.messaging.MessageHandler;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import com.demo.sftp.models.Note;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

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

  // @Bean
  // public SftpInboundFileSynchronizer sftpInboundFileSynchronizer() {
  // SftpInboundFileSynchronizer fileSynchronizer = new
  // SftpInboundFileSynchronizer(sftpSessionFactory());
  // fileSynchronizer.setDeleteRemoteFiles(false);
  // fileSynchronizer.setRemoteDirectory(remoteDir);
  // fileSynchronizer.setFilter(new SftpSimplePatternFileListFilter("*.xml"));
  // return fileSynchronizer;
  // }

  // @Bean
  // @InboundChannelAdapter(channel = "sftpChannel", poller = @Poller(fixedDelay =
  // "5000"))
  // public MessageSource<File> sftpMessageSource() {
  // SftpInboundFileSynchronizingMessageSource source =
  // new SftpInboundFileSynchronizingMessageSource(sftpInboundFileSynchronizer());
  // source.setLocalDirectory(new File("sftp-inbound"));
  // source.setAutoCreateLocalDirectory(true);
  // source.setLocalFilter(new AcceptOnceFileListFilter<File>());
  // source.setMaxFetchSize(1);
  // return source;
  // }

  // @Bean
  // @ServiceActivator(inputChannel = "sftpChannel")
  // public MessageHandler handler() {
  // return new MessageHandler() {

  // @Override
  // public void handleMessage(Message<?> message) throws MessagingException {
  // System.out.println(message.getPayload());
  // }

  // };
  // }

  @Bean
  @InboundChannelAdapter(channel = "stream")
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
  @Transformer(inputChannel = "rawdata", outputChannel = "data")
  public UnmarshallingTransformer marshallingTransformer() {
    Jaxb2Marshaller jaxb2Marshaller = new Jaxb2Marshaller();
    jaxb2Marshaller.setClassesToBeBound(Note.class);

    return new UnmarshallingTransformer(jaxb2Marshaller);
  }

  @Bean
  public SftpRemoteFileTemplate template() {
    return new SftpRemoteFileTemplate(sftpSessionFactory());
  }

  @ServiceActivator(inputChannel = "data", adviceChain = "after")
  @Bean
  public MessageHandler handle() throws JsonMappingException, JsonProcessingException {
    // Note note = xmlMapper.readValue(<>, Note.class);
    // System.out.println(note);
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
