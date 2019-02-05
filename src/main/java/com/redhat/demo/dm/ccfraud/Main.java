package com.redhat.demo.dm.ccfraud;
import com.google.gson.Gson;
import com.redhat.demo.dm.ccfraud.domain.CreditCardTransaction;
import com.redhat.demo.dm.ccfraud.domain.PotentialFraudFact;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.EntryPoint;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.time.SessionClock;
import org.kie.api.time.SessionPseudoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class Main {

    /**
     * Example about Kafka consumer and producer creation
     *
     * @param vertx
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd:HHmmssSSS");



    private static KieContainer kieContainer;

    private static CreditCardTransactionRepository cctRepository = new InMemoryCreditCardTransactionRepository();


    public static void main(String args[]) {
        KieServices KIE_SERVICES = KieServices.Factory.get();
        System.out.print("KIE_SERVICES"+KIE_SERVICES);
        // Load the Drools KIE-Container.
        kieContainer = KIE_SERVICES.newKieClasspathContainer();
       Main creditCardFraudVerticle = new Main();
       creditCardFraudVerticle.exampleCreateConsumerJava(Vertx.vertx());
   }
    public void exampleCreateConsumerJava(Vertx vertx) {

        // creating the consumer using properties config
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test");


        // use consumer for interacting with Apache Kafka
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);


        // subscribe to several topics
        Set<String> topics = new HashSet<>();
        topics.add("test-vertx");

        consumer.subscribe(topics);

        // or just subscribe to a single topic
        consumer.subscribe(topics, ar -> {
            if (ar.succeeded()) {
                System.out.println("subscribed");
            } else {
                System.out.println("Could not subscribe " + ar.cause().getMessage());
            }
        });

        consumer.handler(record -> {
            System.out.println(new Gson().fromJson(record.value(),CreditCardTransaction.class));
            CreditCardTransaction creditCardTransaction = new Gson().fromJson(record.value(),CreditCardTransaction.class);
            processTransaction(creditCardTransaction);
        });


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
        kieSession.fireAllRules();

        Collection<?> fraudResponse = kieSession.getObjects();

        for(Object object: fraudResponse) {
            String jsonString = new Gson().toJson(object);
            PotentialFraudFact potentialFraudFact = new Gson().fromJson(jsonString,PotentialFraudFact.class);
            System.out.print("PotentialFraudFact"+potentialFraudFact);

			CaseMgmt caseMgmt = new CaseMgmt();
			caseMgmt.invokeCase(potentialFraudFact);
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
