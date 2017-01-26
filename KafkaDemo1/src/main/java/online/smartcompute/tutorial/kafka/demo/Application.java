package online.smartcompute.tutorial.kafka.demo;

import java.util.Arrays;
import java.util.Calendar;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackages = "online.smartcompute.tutorial.kafka.demo")
public class Application {
	
	public static void main(String[] args) {
		ApplicationContext context = null;
		try {
			context = new AnnotationConfigApplicationContext(Application.class);
			Application p = context.getBean(Application.class);
			p.start();
		} finally {
			//((AnnotationConfigApplicationContext)context).close();
		}
	}

	@Autowired
	private KafkaProducer<String, String> producer;

	@Autowired
	private KafkaConsumer<String, String> consumer;
	
	private static String TOPIC_NAME = "my-replicated-topic"; 
	
	private void start() {
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				Integer i = 0;
				while (true) {
					producer.send(new ProducerRecord<String, String>(TOPIC_NAME, Integer.toString(i),"Hello from Masud at " + Calendar.getInstance().getTime()));
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		});
		t1.start();
		
		Thread t2 = new Thread(new Runnable() {
			/*
			 * (non-Javadoc)
			 * @see java.lang.Runnable#run()
			 */
			public void run() {
				 consumer.subscribe(Arrays.asList(TOPIC_NAME));
				 while(true){
					 ConsumerRecords<String, String> records =	consumer.poll(100); 
					 for(ConsumerRecord<String,String> record : records){
						 	System.out.println("offset : " + record.offset() + " key : " + record.key()  + " value : " + record.value());
					 }
				 }
			}
			
		});
		t2.start();
	}
}