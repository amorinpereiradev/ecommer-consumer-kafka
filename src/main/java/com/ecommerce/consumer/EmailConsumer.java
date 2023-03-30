package com.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import com.ecommerce.dtos.EmailMessageDto;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class EmailConsumer {

	@Value("${spring.mail.username}")
	private String from;
	
	@Autowired
	private JavaMailSender javaMailSender;
	
	
	@KafkaListener(topics = "${topic.name.consumer}", groupId = "group-id")
	public void consumer(ConsumerRecord<String, String> record) throws Exception{
		
		ObjectMapper objectMapper = new ObjectMapper();
		
		EmailMessageDto dto = objectMapper.readValue(record.value(), EmailMessageDto.class);
		
		SimpleMailMessage message = new SimpleMailMessage();
		
		message.setFrom(from);
		message.setTo(dto.getTo());
		message.setSubject(dto.getSubject());
		message.setText(dto.getBody());
		
		javaMailSender.send(message);
		
	}
	
}
