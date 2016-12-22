package online.smartcompute.tutorial.kafka.demo;

import java.util.Calendar;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackages = "online.smartcompute.tutorial.kafka.demo")
public class Application {

	public static void main(String[] args) {
		try {
			ApplicationContext context = new AnnotationConfigApplicationContext(Application.class);
			Application p = context.getBean(Application.class);
			p.start();
		} finally {
			//TODO
		}
	}

	@Autowired
	private KafkaProducer<String, String> producer;

	private void start() {
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				Integer i = 0;
				while (true) {
					producer.send(new ProducerRecord<String, String>("topic1", Integer.toString(i),"Hello from Masud at " + Calendar.getInstance().getTime()));
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
	}

}