package com.redhat.demo.dm.ccfraud;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.redhat.demo.dm.ccfraud.domain.PotentialFraudFact;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.kie.api.KieServices;
import org.kie.api.runtime.ClassObjectFilter;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.EntryPoint;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.time.SessionClock;
import org.kie.api.time.SessionPseudoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.demo.dm.ccfraud.domain.CountryCode;
import com.redhat.demo.dm.ccfraud.domain.CreditCardTransaction;
import com.redhat.demo.dm.ccfraud.domain.Terminal;

/**
 * Main class of the demo project wich creates a new {@link CreditCardTransaction}, loads the previous transactions from a CSV file and uses
 * the Drools CEP engine to determine whether there was a potential fraud with the transactions.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class Main {

	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd:HHmmssSSS");



	private static KieContainer kieContainer;

	private static CreditCardTransactionRepository cctRepository = new InMemoryCreditCardTransactionRepository();

	public static void main(String[] args) {
		try {
			 KieServices KIE_SERVICES = KieServices.Factory.get();
			 System.out.print("KIE_SERVICES"+KIE_SERVICES);
			// Load the Drools KIE-Container.
			kieContainer = KIE_SERVICES.newKieClasspathContainer();

			long transactionTime = 0L;
			try {
				transactionTime = DATE_FORMAT.parse("20180629:094000000").getTime();
			} catch (ParseException pe) {
				throw new RuntimeException(pe);
			}

			// Define the new incoming credit-card transaction. In an actual system, this event would come a Kafka stream or a Vert.x EventBus
			// event.

			Properties props = new Properties();
			props.put("bootstrap.servers", "kafka:9092");
			props.put("group.id", "test");
			props.put("enable.auto.commit", "true");
			props.put("auto.commit.interval.ms", "1000");
			props.put("session.timeout.ms", "30000");
			props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
			props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


			KafkaConsumer consumer = new KafkaConsumer(props);
			consumer.subscribe(Arrays.asList("events"));
			int counter = 0;


			List<CreditCardTransaction> transactions = new ArrayList<>();
			CreditCardTransaction creditCardTransaction = null;


			try {
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(1000);
					for (ConsumerRecord<String, String> record : records) {
						creditCardTransaction = new Gson().fromJson(record.value(), CreditCardTransaction.class);
						processTransaction(creditCardTransaction);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();

			} finally {
				consumer.close();
			}



		}catch(Exception e) {
			e.printStackTrace();
		}

	}

	private static void processTransaction(CreditCardTransaction ccTransaction) {
		// Retrieve all transactions for this account
		Collection<CreditCardTransaction> ccTransactions = cctRepository
				.getCreditCardTransactionsForCC(ccTransaction.getCreditCardNumber());

		if(ccTransactions == null) {
			return;
		}
		System.out.println("Found '" + ccTransactions.size() + "' transactions for creditcard: '" + ccTransaction.getCreditCardNumber() + "'.");

		KieSession kieSession = kieContainer.newKieSession();
		// Insert transaction history/context.
		System.out.println("Inserting credit-card transaction context into session.");
		for (CreditCardTransaction nextTransaction : ccTransactions) {
			insert(kieSession, "Transactions", nextTransaction);
		}
		// Insert the new transaction event
		System.out.println("Inserting credit-card transaction event into session.");
		insert(kieSession, "Transactions", ccTransaction);
		// And fire the com.redhat.demo.dm.com.redhat.demo.dm.ccfraud.rules.
		int k = kieSession.fireAllRules();
		System.out.println("Rule Fired"+k);
		Collection<?> fraudResponse = kieSession.getObjects();

		for(Object object: fraudResponse) {
			System.out.println("Object Check");
			String jsonString = new Gson().toJson(object);
			System.out.println("Object Check"+jsonString);
			try{
			PotentialFraudFact potentialFraudFact = new Gson().fromJson(jsonString,PotentialFraudFact.class);
			System.out.print("PotentialFraudFact"+potentialFraudFact);
			CaseMgmt caseMgmt = new CaseMgmt();
			caseMgmt.invokeCase(potentialFraudFact);
			}catch(Exception e){
			e.printStackTrace();
			}
			
		}



		// Dispose the session to free up the resources.
		kieSession.dispose();

	}

	/**
	 * CEP insert method that inserts the event into the Drools CEP session and programatically advances the session clock to the time of
	 * the current event.
	 * 
	 * @param kieSession
	 *            the session in which to insert the event.
	 * @param stream
	 *            the name of the Drools entry-point in which to insert the event.
	 * @param cct
	 *            the event to insert.
	 * 
	 * @return the {@link FactHandle} of the inserted fact.
	 */
	private static FactHandle insert(KieSession kieSession, String stream, CreditCardTransaction cct) {
		SessionClock clock = kieSession.getSessionClock();
		if (!(clock instanceof SessionPseudoClock)) {
			String errorMessage = "This fact inserter can only be used with KieSessions that use a SessionPseudoClock";
			LOGGER.error(errorMessage);
			throw new IllegalStateException(errorMessage);
		}
		SessionPseudoClock pseudoClock = (SessionPseudoClock) clock;
		EntryPoint ep = kieSession.getEntryPoint(stream);

		// First insert the event
		FactHandle factHandle = ep.insert(cct);
		// And then advance the clock.

		long advanceTime = cct.getTimestamp() - pseudoClock.getCurrentTime();
		if (advanceTime > 0) {
			System.out.println("Advancing the PseudoClock with " + advanceTime + " milliseconds.");
			pseudoClock.advanceTime(advanceTime, TimeUnit.MILLISECONDS);
		} else {
			// Print a warning when we don't need to advance the clock. This usually means that the events are entering the system in the
			// incorrect order.
			LOGGER.warn("Not advancing time. CreditCardTransaction timestamp is '" + cct.getTimestamp() + "', PseudoClock timestamp is '"
					+ pseudoClock.getCurrentTime() + "'.");
		}
		return factHandle;
	}
}
