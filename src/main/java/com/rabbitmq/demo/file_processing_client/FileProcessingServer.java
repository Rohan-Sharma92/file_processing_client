package com.rabbitmq.demo.file_processing_client;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

public class FileProcessingServer {

	private Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

	public static void main(String[] args) {
		FileProcessingServer server = new FileProcessingServer();
		server.connect();
	}

	public void connect() {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		String url = System.getenv("url");
		String type = System.getenv("type");
		logger.info("Configuration: URL-"+url +" , type -"+type);
		String routingKey = System.getenv("routing_key");
		try {
			connectionFactory.setUri(url);
			Connection connection = connectionFactory.newConnection();
			Channel channel = connection.createChannel();
			String queueName = channel.queueDeclare().getQueue();
			channel.queueBind(queueName, "amq.rabbitmq.event", "exchange.*");
			Consumer consumer = new DefaultConsumer(channel) {
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
						byte[] body) throws IOException {
					String event = envelope.getRoutingKey();
					Map<String, Object> headers = properties.getHeaders();
					String exchange = headers.get("name").toString();

					if (event.equals("exchange.created")) {
						logger.info("Created: " + exchange);
						if (exchange.equals(type)) {
							logger.info("Starting subscription");
							startSubscription(channel, type, routingKey);
						}
					} else  {
						logger.info("Deleted: " + exchange);
					}
				}
			};
			channel.basicConsume(queueName, true, consumer);
		} catch (Exception e) {
			logger.error("Caught exception:",e);
		}
	}

	protected void startSubscription(Channel channel, String type, String routingKey) throws IOException {
		String queueName = routingKey + "_" + type;
		channel.queueDeclare(queueName, false, false, false, null);
		channel.queueBind(queueName, type, routingKey);
		channel.basicConsume(queueName, new Consumer() {

			@Override
			public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {

			}

			@Override
			public void handleRecoverOk(String consumerTag) {

			}

			@Override
			public void handleDelivery(String arg0, Envelope arg1, BasicProperties arg2, byte[] arg3)
					throws IOException {
				String msg = new String(arg3, Charset.defaultCharset());
				logger.info("Received Message:" + msg);
			}

			@Override
			public void handleConsumeOk(String consumerTag) {

			}

			@Override
			public void handleCancelOk(String consumerTag) {

			}

			@Override
			public void handleCancel(String consumerTag) throws IOException {

			}
		});

	}

}
