/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2017, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * Simple Apache Kafka producer
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"


int is_ssl_enabled = 1;

void enable_ssl_config(rd_kafka_conf_t *conf)
{

	char errstr[512];       /* librdkafka API error reporting buffer */

        if (rd_kafka_conf_set(conf, "security.protocol", "SSL",
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                assert(0);
        }

        if (rd_kafka_conf_set(conf, "ssl.ca.location", "/Users/pratnaik/Desktop/kafka-builds/nxkafka/librdkafka/certificate/ca-cert",
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                assert(0);
        }

        if (rd_kafka_conf_set(conf, "ssl.certificate.location", "/Users/pratnaik/Desktop/kafka-builds/nxkafka/librdkafka/certificate/client_localhost_client.pem",
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                assert(0);
        }

        if (rd_kafka_conf_set(conf, "ssl.key.location", "/Users/pratnaik/Desktop/kafka-builds/nxkafka/librdkafka/certificate/client_localhost_client.key",
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                assert(0);
        }
}


/**
 * @brief Message delivery report callback.
 *
 * This callback is called exactly once per message, indicating if
 * the message was succesfully delivered
 * (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
 * failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).
 *
 * The callback is triggered from rd_kafka_poll() and executes on
 * the application's thread.
 */
static void dr_msg_cb (rd_kafka_t *rk,
                       const rd_kafka_message_t *rkmessage, void *opaque) {
        if (rkmessage->err)
                fprintf(stderr, "%% Message delivery failed: %s\n",
                        rd_kafka_err2str(rkmessage->err));
        else
                fprintf(stderr,
                        "%% Message delivered (%zd bytes, "
                        "partition %"PRId32")\n",
                        rkmessage->len, rkmessage->partition);

        /* The rkmessage is destroyed automatically by librdkafka */
}

static int msgs_wait = 0; /* bitmask */

/**
 * Delivery report callback.
 * Called for each message once to signal its delivery status.
 */
static void dr_cb (rd_kafka_t *rk, void *payload, size_t len,
		   rd_kafka_resp_err_t err, void *opaque, void *msg_opaque) {
	int msgid = *(int *)msg_opaque;

	free(msg_opaque);

	if (err)
		printf("Unexpected delivery error for message #%i: %s\n",
			  msgid, rd_kafka_err2str(err));

	if (!(msgs_wait & (1 << msgid)))
		printf("Unwanted delivery report for message #%i "
			  "(waiting for 0x%x)\n", msgid, msgs_wait);

	printf("Delivery report for message #%i: %s\n",
		 msgid, rd_kafka_err2str(err));

	msgs_wait &= ~(1 << msgid);
}


// Test1 : Connect to broker success , data sent on topic successfully and report success.
void test_1(const char *brokers, const char *topic)
{

	printf("TEST 1 started.\n");
	rd_kafka_t *rk;         /* Producer instance handle */
	rd_kafka_conf_t *conf;  /* Temporary configuration object */
	char errstr[512];       /* librdkafka API error reporting buffer */
	char buf[512];          /* Message value temporary buffer */

	conf = rd_kafka_conf_new();

	if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
				errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		fprintf(stderr, "%s\n", errstr);
		assert(0);
	}

        if(is_ssl_enabled)
        {
                enable_ssl_config(conf);
        }

	rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk) {
		fprintf(stderr,
				"%% Failed to create new producer: %s\n", errstr);
		assert(0);
	}
	
	snprintf(buf,512,"Test 1 :  test messege from producer");

	size_t len = strlen(buf);
	rd_kafka_resp_err_t err;

	if (buf[len-1] == '\n') /* Remove newline */
		buf[--len] = '\0';

	if (len == 0) {
		rd_kafka_poll(rk, 0/*non-blocking */);
		return;
	}

	err = rd_kafka_producev(
			/* Producer handle */
			rk,
			/* Topic name */
			RD_KAFKA_V_TOPIC(topic),
			/* Make a copy of the payload. */
			RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
			/* Message value and length */
			RD_KAFKA_V_VALUE(buf, len),
			/* Per-Message opaque, provided in
			 * delivery report callback as
			 * msg_opaque. */
			RD_KAFKA_V_OPAQUE(NULL),
			/* End sentinel */
			RD_KAFKA_V_END);

	if (err) {
		fprintf(stderr,
				"%% Failed to produce to topic %s: %s\n",
				topic, rd_kafka_err2str(err));
		assert(0);

	} else {
		fprintf(stderr, "%% Enqueued message (%zd bytes) "
				"for topic %s\n",
				len, topic);
	}

	rd_kafka_poll(rk, 0/*non-blocking*/);
	rd_kafka_flush(rk, 10*1000 /* wait for max 10 seconds */);

	if (rd_kafka_outq_len(rk) > 0)
		fprintf(stderr, "%% %d message(s) were not delivered\n",
				rd_kafka_outq_len(rk));

	rd_kafka_destroy(rk);

	printf("TEST 1 successful.\n");
}

// Tests multiple rd_kafka_t object creations and destructions.
void test_2(const char *brokers, const char *topic)
{

	int partition = RD_KAFKA_PARTITION_UA; /* random */
	int i;
	const int NUM_ITER = 10;
	char errstr[512];
	printf("TEST 2 started.\n");
	printf("Creating and destroying %i kafka instances\n", NUM_ITER);

	/* Create, use and destroy NUM_ITER kafka instances. */
	for (i = 0 ; i < NUM_ITER ; i++) {
		rd_kafka_t *rk;
		rd_kafka_topic_t *rkt;
		rd_kafka_conf_t *conf;
		rd_kafka_topic_conf_t *topic_conf;
		char msg[128];

	        conf = rd_kafka_conf_new();
        	topic_conf = rd_kafka_topic_conf_new();

                if (!topic)
			assert(0);

	        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
        	                        errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                	fprintf(stderr, "%s\n", errstr);
                	assert(0);
        	}
  	
		if(is_ssl_enabled)
        	{
                	enable_ssl_config(conf);
        	}

		rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
		if (!rk) {
			fprintf(stderr,
					"%% Failed to create new producer: %s\n", errstr);
			assert(0);
		}

		rkt = rd_kafka_topic_new(rk, topic, topic_conf);
		if (!rkt)
		{	printf("Failed to create topic for "
				  "rdkafka instance #%i\n",
				  i);
			assert(0);
		}
		
		snprintf(msg, sizeof(msg), "%s test message for iteration #%i","T2: ", i);

		/* Produce a message */
		rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
				 msg, strlen(msg), NULL, 0, NULL);
		
		/* Wait for it to be sent (and possibly acked) */
		rd_kafka_flush(rk, -1);

		/* Destroy topic */
		rd_kafka_topic_destroy(rkt);

		/* Destroy rdkafka instance */
		rd_kafka_destroy(rk);
	}

	printf("TEST 2 successful.\n");
}


// Test 3
// producer trying to connect to broker but no response or error response from broker
void test_3(const char *brokers, const char *topic)
{

	int partition = RD_KAFKA_PARTITION_UA; /* random */
	char errstr[512];
	int r;
	int timeout=1000; //ms
	printf("TEST 3 started.\n");
	printf("Producer trying to connect to broker but no response or error response from broker\n");

	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char msg[128];
	const struct rd_kafka_metadata *metadata;
	conf = rd_kafka_conf_new();
	topic_conf = rd_kafka_topic_conf_new();

	if (!topic)
		assert(0);


	if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
				errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		fprintf(stderr, "%s\n", errstr);
		assert(0);
	}

	if(is_ssl_enabled)
	{       
		enable_ssl_config(conf);
	}

	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk) {
		fprintf(stderr,
				"%% Failed to create new producer: %s\n", errstr);
		assert(0);
	}

	rkt = rd_kafka_topic_new(rk, topic, topic_conf);
	if (!rkt)         
	{       printf("Failed to create topic for "
			"rdkafka instance\n");
		assert(0);
	}

        if ((r = rd_kafka_metadata(rk, 0, rkt, &metadata,timeout)) != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
                printf("TEST 3: Failed to connect to broker , errcode %d\n",r);
                rd_kafka_metadata_destroy(metadata);
                goto destroy;
        }
        else
        {
                printf("TEST 3: Testcase was supoosed to fail to connect to broker but instead its sending data!! ERROR\n");
        }

	snprintf(msg, sizeof(msg), "Test messge for TEST 3");

	/* Produce a message */
	rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
			msg, strlen(msg), NULL, 0, NULL);


destroy:

	/* Wait for it to be sent (and possibly acked) */
	rd_kafka_flush(rk, -1);

	/* Destroy topic */
	rd_kafka_topic_destroy(rkt);

	/* Destroy rdkafka instance */
	rd_kafka_destroy(rk);
	printf("TEST 3 successful.\n");

}

static void test_4_dr_cb (rd_kafka_t *rk, void *payload, size_t len,
		   rd_kafka_resp_err_t err, void *opaque, void *msg_opaque) {
	int msgid = *(int *)msg_opaque;

	free(msg_opaque);

	printf("Delivery report for message #%i: %s\n",
		 msgid, rd_kafka_err2str(err));

	if (err != RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
        {		
		printf("Message #%i failed with unexpected error %s\n",msgid, rd_kafka_err2str(err));
		assert(0);
	}
}

// Test4 : Produce to unknown partition
// https://github.com/edenhill/librdkafka/blob/master/tests/0002-unkpart.c
void test_4(const char *brokers, const char *topic)
{

	printf("TEST 4 started.\n");
	printf("Test scenario  : Producer trying to produce to unknown partition\n");
	int partition = 99; /* non-existent */
	int r;
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char msg[128];
	char errstr[512];
	int i=0;

        const struct rd_kafka_metadata *metadata;
	
	conf = rd_kafka_conf_new();
        topic_conf = rd_kafka_topic_conf_new();

        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                assert(0);
        }

        if(is_ssl_enabled)
        {
                enable_ssl_config(conf);
        }

	
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr,
                                "%% Failed to create new producer: %s\n", errstr);
                assert(0);
        }

	rkt = rd_kafka_topic_new(rk, topic, topic_conf);
	if (!rkt)
	{       printf("Failed to create topic for "
			"rdkafka instance\n");
	assert(0);
	}


	/* Set delivery report callback */
	rd_kafka_conf_set_dr_cb(conf, test_4_dr_cb);

	/* Request metadata so that we know the cluster is up before producing
	 * messages, otherwise erroneous partitions will not fail immediately.*/
	if ((r = rd_kafka_metadata(rk, 0, rkt, &metadata,1000)) != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
		printf("Failed to acquire metadata: %s\n",rd_kafka_err2str(r));
		assert(0);
	}
	rd_kafka_metadata_destroy(metadata);

	/* Produce a message */
	int *msgidp = malloc(sizeof(*msgidp));
	*msgidp = i;
	snprintf(msg, sizeof(msg), "%s test message #%i", __FUNCTION__, i);
	r = rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
			msg, strlen(msg), NULL, 0, msgidp);
	if (r == -1) {
		if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
			printf("Failed to produce message #%i: "
					"unknown partition: good! expected result\n", i);
		else
			printf("Failed to produce message #%i: %s\n",
					i, rd_kafka_err2str(rd_kafka_last_error()));
		free(msgidp);
	} else {
		printf("Messege Produced");
	}

	/* Wait for messages to time out */
	rd_kafka_flush(rk, -1);

	/* Destroy topic */
	rd_kafka_topic_destroy(rkt);

	/* Destroy rdkafka instance */
	printf("Destroying kafka instance %s\n", rd_kafka_name(rk));
	rd_kafka_destroy(rk);
	printf("TEST 4 successful.\n");
}

//test 5
// Produce to unknown topic
// https://github.com/edenhill/librdkafka/blob/master/tests/1000-unktopic.c
void test_5(const char *brokers, const char *topic)
{       

	printf("TEST 5 started.\n");
	printf("Test scenario  : Producer trying to produce to unknown topic\n");
	int partition = RD_KAFKA_PARTITION_UA; 
	int r;
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char msg[128];
	char errstr[512];
	int i=0;

	const struct rd_kafka_metadata *metadata;

	conf = rd_kafka_conf_new();
	topic_conf = rd_kafka_topic_conf_new();


        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                assert(0);
        }

        if(is_ssl_enabled)
        {
                enable_ssl_config(conf);
        }


        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr,
                                "%% Failed to create new producer: %s\n", errstr);
                assert(0);
        }

        rkt = rd_kafka_topic_new(rk, "Random123", topic_conf);
        if (!rkt)
        {       printf("Failed to create topic for "
                        "rdkafka instance\n");
        	assert(0);
        }


	/* Set delivery report callback */
	rd_kafka_conf_set_dr_cb(conf, test_4_dr_cb);

	/* Request metadata so that we know the cluster is up before producing
	 * messages, otherwise erroneous partitions will not fail immediately.*/
	if ((r = rd_kafka_metadata(rk, 0, rkt, &metadata,1000)) != RD_KAFKA_RESP_ERR_NO_ERROR)
	{       
		printf("Failed to acquire metadata: %s\n",rd_kafka_err2str(r));
		assert(0);
	}
	rd_kafka_metadata_destroy(metadata);

	/* Produce a message */
	int *msgidp = malloc(sizeof(*msgidp));
	*msgidp = i;
	snprintf(msg, sizeof(msg), "%s test message #%i", __FUNCTION__, i);
	r = rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
			msg, strlen(msg), NULL, 0, msgidp);
	if (r == -1) {
		if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
			printf("Failed to produce message #%i: "
					"unknown topic: good! expected result.\n", i);
		else
			printf("Failed to produce message #%i: %s\n",
					i, rd_kafka_err2str(rd_kafka_last_error()));
		free(msgidp);
	} else {
		printf("Messege Produced");
	}

	/* Wait for messages to time out */
	rd_kafka_flush(rk, -1);
	/* Destroy topic */
	rd_kafka_topic_destroy(rkt);

	/* Destroy rdkafka instance */
	printf("Destroying kafka instance %s\n", rd_kafka_name(rk));
	rd_kafka_destroy(rk);

	printf("TEST 5 successful.\n");

}

// Test6 : Data sent was greater than max allowed limit, handle failure.
// https://github.com/edenhill/librdkafka/blob/master/tests/0003-msgmaxsize.c
void test_6(const char *brokers, const char *topic)
{
	printf("TEST 6 started.\n");
	printf("Data sent was greater than max allowed limit, handle failure.\n");
	int partition = 0;
	int r;
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char errstr[512];
	char *msg;
	static const int msgsize = 100000;
	int msgcnt = 10;
	int i;        
	
	conf = rd_kafka_conf_new();
	/* Topic configuration */
	topic_conf = rd_kafka_topic_conf_new();

        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                assert(0);
        }

        if(is_ssl_enabled)
        {
                enable_ssl_config(conf);
        }


	/* Set a small maximum message size. */
	if (rd_kafka_conf_set(conf, "message.max.bytes", "100000",
			      errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{		
		printf("TEST 6: failed to set config");
		assert(0);
	}
	/* Set delivery report callback */
	rd_kafka_conf_set_dr_cb(conf, dr_cb);

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr,
                                "%% Failed to create new producer: %s\n", errstr);
                assert(0);
        }

        rkt = rd_kafka_topic_new(rk, topic, topic_conf);
        if (!rkt)
        {       printf("Failed to create topic for "
                        "rdkafka instance\n");
        	assert(0);
        }

	topic_conf = NULL; /* Now owned by topic */

	msg = calloc(1, msgsize);

	/* Produce 'msgcnt' messages, size odd ones larger than max.bytes,
	 * and even ones smaller than max.bytes. */
	for (i = 0 ; i < msgcnt ; i++) {
		int *msgidp = malloc(sizeof(*msgidp));
		size_t len;
		int toobig = i & 1;

		*msgidp = i;
		if (toobig) {
			/* Too big */
			len = 200000;
		} else {
			/* Good size */
			len = 5000;
			msgs_wait |= (1 << i);
		}

		printf(msg, msgsize, "%s test message #%i", brokers, i);
		r = rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
				     msg, len, NULL, 0, msgidp);

		if (toobig) {
			if (r != -1)
			{
				printf("TEST 6 : Succeeded to produce too large message #%i\n", i);
				assert(0);
			}
			else
			{
				printf("TEST 6: Too large messege hence rejecting the request to produce #%i\n", i);
			}
			free(msgidp);
		} else if (r == -1)
		{
			printf("TEST 6 : Failed to produce message #%i: %s\n",i, rd_kafka_err2str(r));
			assert(0);
		}
	}

	/* Wait for messages to be delivered. */
	while (rd_kafka_outq_len(rk) > 0)
		rd_kafka_poll(rk, 50);

	if (msgs_wait != 0)
		printf("Still waiting for messages: 0x%x\n", msgs_wait);

	free(msg);

	/* Destroy topic */
	rd_kafka_topic_destroy(rkt);
		
	/* Destroy rdkafka instance */
	printf("Destroying kafka instance %s\n", rd_kafka_name(rk));
	rd_kafka_destroy(rk);

	printf("TEST 6 successful.\n");
}

static int msgid_next = 0;

static void test_7_dr_cb (rd_kafka_t *rk, void *payload, size_t len, rd_kafka_resp_err_t err, void *opaque, void *msg_opaque) {
	int msgid = *(int *)msg_opaque;

	free(msg_opaque);

	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
		printf(" TEST 7: Message delivery failed: %s\n", rd_kafka_err2str(err));

	if (msgid != msgid_next) {
		printf("TEST 7 : Delivered msg %i, expected %i\n",msgid, msgid_next);
		return;
	}
	
	printf("TEST 7: messge %d recieved properly\n", msgid);
	msgid_next = msgid+1;
}



// test 7
// produce in order : https://github.com/edenhill/librdkafka/blob/master/tests/0005-order.c
void test_7(const char *brokers, const char *topic)
{
	printf("TEST 7 started.\n");
	printf("Test scenario  : Producer trying to produce in order \n");
        int partition = RD_KAFKA_PARTITION_UA;
        int r;
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        rd_kafka_conf_t *conf;
        rd_kafka_topic_conf_t *topic_conf;
        char msg[128];
        char errstr[512];
        //int i=0;

        const struct rd_kafka_metadata *metadata;

        conf = rd_kafka_conf_new();
        topic_conf = rd_kafka_topic_conf_new();

        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                assert(0);
        }

        if(is_ssl_enabled)
        {
                enable_ssl_config(conf);
        }

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr,
                                "%% Failed to create new producer: %s\n", errstr);
                assert(0);
        }

        rkt = rd_kafka_topic_new(rk, topic, topic_conf);
        if (!rkt)
        {       printf("Failed to create topic for "
                        "rdkafka instance\n");
                assert(0);
        }

        /* Set delivery report callback */
        rd_kafka_conf_set_dr_cb(conf, test_7_dr_cb);

        /* Request metadata so that we know the cluster is up before producing
         * messages, otherwise erroneous partitions will not fail immediately.*/
        if ((r = rd_kafka_metadata(rk, 0, rkt, &metadata,1000)) != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
                printf("Failed to acquire metadata: %s\n",rd_kafka_err2str(r));
                assert(0);
        }
        rd_kafka_metadata_destroy(metadata);
	int msgcnt = 50;
	
	for (int i = 0 ; i < msgcnt ; i++) {
		/* Produce a message */
		int *msgidp = malloc(sizeof(*msgidp));
		*msgidp = i;
		snprintf(msg, sizeof(msg), "%s test message #%i", __FUNCTION__, i);
		r = rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
				msg, strlen(msg), NULL, 0, msgidp);
		if (r == -1) {
			if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
				printf("Failed to produce message #%i: "
						"unknown topic: good!\n", i);
			else
				printf("Failed to produce message #%i: %s\n",
						i, rd_kafka_err2str(rd_kafka_last_error()));
			free(msgidp);
		} else {
			printf("Messege Produced\n");
		}

	}

	printf(" TEST 7 : Produced %i messages, waiting for deliveries\n", msgcnt);

        /* Wait for messages to time out */
        rd_kafka_flush(rk, -1);
        /* Destroy topic */
        rd_kafka_topic_destroy(rkt);

        /* Destroy rdkafka instance */
        printf("Destroying kafka instance %s\n", rd_kafka_name(rk));
        rd_kafka_destroy(rk);
	printf("TEST 7 successful.\n");

}

// SSL config on client
void test_8(const char *brokers, const char *topic)
{

	printf("TEST 8 started.\n");
        printf("Config SSL on client node , data sent on topic successfully and report success.\n");
        rd_kafka_t *rk;         /* Producer instance handle */
        rd_kafka_conf_t *conf;  /* Temporary configuration object */
        char errstr[512];       /* librdkafka API error reporting buffer */
        char buf[512];          /* Message value temporary buffer */

        conf = rd_kafka_conf_new();

/*        if (rd_kafka_conf_set(conf, "debug", "security,broker",
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                assert(0);
        }
*/
        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                assert(0);
        }
	
	if(is_ssl_enabled)
	{	
		enable_ssl_config(conf);
	}

/*
        if (rd_kafka_conf_set(conf, "ssl.key.password", "abcdefgh",
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                assert(0);
        }
*/
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr,
                                "%% Failed to create new producer: %s\n", errstr);
                assert(0);
        }

        snprintf(buf,512,"Test 8 :  test messege from producer");

        size_t len = strlen(buf);
        rd_kafka_resp_err_t err;

        if (buf[len-1] == '\n') /* Remove newline */
                buf[--len] = '\0';

        if (len == 0) {
                rd_kafka_poll(rk, 0/*non-blocking */);
                return;
        }

        err = rd_kafka_producev(
                        /* Producer handle */
                        rk,
                        /* Topic name */
                        RD_KAFKA_V_TOPIC(topic),
                        /* Make a copy of the payload. */
                        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                        /* Message value and length */
                        RD_KAFKA_V_VALUE(buf, len),
                        /* Per-Message opaque, provided in
                         * delivery report callback as
                         * msg_opaque. */
                        RD_KAFKA_V_OPAQUE(NULL),
                        /* End sentinel */
                        RD_KAFKA_V_END);

        if (err) {
                fprintf(stderr,
                                "%% Failed to produce to topic %s: %s\n",
                                topic, rd_kafka_err2str(err));
                assert(0);

        } else {
                fprintf(stderr, "%% Enqueued message (%zd bytes) "
                                "for topic %s\n",
                                len, topic);
        }

        rd_kafka_poll(rk, 0/*non-blocking*/);
        rd_kafka_flush(rk, 10*1000 /* wait for max 10 seconds */);

        if (rd_kafka_outq_len(rk) > 0)
                fprintf(stderr, "%% %d message(s) were not delivered\n",
                                rd_kafka_outq_len(rk));

        rd_kafka_destroy(rk);
	printf("TEST 8 successful.\n");
}

// Producer sending data continuously 
void test_9(const char *brokers, const char *topic)
{

	printf("TEST 9 started.\n");
        printf("Producer sending data continuously, data sent on topic successfully and report success.\n");
        rd_kafka_t *rk;         /* Producer instance handle */
        rd_kafka_conf_t *conf;  /* Temporary configuration object */
        char errstr[512];       /* librdkafka API error reporting buffer */
        char buf[512];          /* Message value temporary buffer */

        conf = rd_kafka_conf_new();

/*        if (rd_kafka_conf_set(conf, "debug", "security,broker",
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                assert(0);
        }
*/
        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                                errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                assert(0);
        }
       
        if(is_ssl_enabled)
        {
                enable_ssl_config(conf);
        }
 
	rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr,
                                "%% Failed to create new producer: %s\n", errstr);
                assert(0);
        }

        snprintf(buf,512,"Test 9 :  Prod#1 test messege from producer");

        size_t len = strlen(buf);
        rd_kafka_resp_err_t err;

        if (buf[len-1] == '\n') /* Remove newline */
                buf[--len] = '\0';

        if (len == 0) {
                rd_kafka_poll(rk, 0/*non-blocking */);
                return;
        }

	int i=0;
	while(i<60)
	{

		sleep(1);
		err = rd_kafka_producev(
				/* Producer handle */
				rk,
				RD_KAFKA_V_PARTITION(0),
				/* Topic name */
				RD_KAFKA_V_TOPIC(topic),
				/* Make a copy of the payload. */
				RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
				/* Message value and length */
				RD_KAFKA_V_VALUE(buf, len),
				/* Per-Message opaque, provided in
				 * delivery report callback as
				 * msg_opaque. */
				RD_KAFKA_V_OPAQUE(NULL),
				/* End sentinel */
				RD_KAFKA_V_END);

		if (err) {
			fprintf(stderr,
					"%% Failed to produce to topic %s: %s\n",
					topic, rd_kafka_err2str(err));
			assert(0);

		} else {
			fprintf(stderr, "%% Enqueued message (%zd bytes) "
					"for topic %s\n",
					len, topic);
		}

		rd_kafka_poll(rk, 0/*non-blocking*/);
		rd_kafka_flush(rk, 10*1000 /* wait for max 10 seconds */);

		if (rd_kafka_outq_len(rk) > 0)
			fprintf(stderr, "%% %d message(s) were not delivered\n",
					rd_kafka_outq_len(rk));
		i++;
	}
        rd_kafka_destroy(rk);
	printf("TEST 9 successful.\n");
}


int main (int argc, char **argv) {

        const char *brokers;    // Argument: broker list 
        const char *topic;      // Argument: topic to produce to 

        if (argc != 3) {
                fprintf(stderr, "%% Usage: %s <broker> <topic>\n", argv[0]);
                return 1;
        }

        printf("Starting test-suite...\n");
        brokers = argv[1];
        topic   = argv[2];

	printf("Broker IP/port : %s, topic name : %s\n",brokers,topic);

	printf("is_ssl_enabled=%d\n",is_ssl_enabled);

	test_1(brokers,topic);
	test_2(brokers,topic);
        test_3(brokers,topic);	
	test_4(brokers,topic);
	test_5(brokers,topic);	
	test_6(brokers,topic);
	test_7(brokers,topic);
	test_8(brokers,topic);
	test_9(brokers,topic);

	printf("Exiting test-suite...\n");
        return 0;
}
