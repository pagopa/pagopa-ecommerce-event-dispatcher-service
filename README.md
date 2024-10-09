# pagopa-ecommerce-event-dispatcher-service

## What is this?

This is a PagoPA microservice that handles scheduled retry mechanism for the eCommerce product.

### Environment variables

These are all environment variables needed by the application:

| Variable name                                                                  | Description                                                                                                                                                                                                                                                        | type    | default       |
|--------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|---------------|
| MONGO_HOST                                                                     | Host where MongoDB instance used to persise events and view resides                                                                                                                                                                                                | string  |               |
| MONGO_USERNAME                                                                 | Username used for connecting to MongoDB instance                                                                                                                                                                                                                   | string  |               |
| MONGO_PASSWORD                                                                 | Password used for connecting to MongoDB instance                                                                                                                                                                                                                   | string  |               |
| MONGO_PORT                                                                     | Port used for connecting to MongoDB instance                                                                                                                                                                                                                       | number  |               |
| MONGO_SSL_ENABLED                                                              | Boolean value indicating if use SSL for connecting to MongoDB instance                                                                                                                                                                                             | boolean |               |
| MONGO_PORT                                                                     | Port used for connecting to MongoDB instance                                                                                                                                                                                                                       | string  |               |
| MONGO_MIN_POOL_SIZE                                                            | Min amount of connections to be retained into connection pool. See docs *                                                                                                                                                                                          | string  |               |
| MONGO_MAX_POOL_SIZE                                                            | Max amount of connections to be retained into connection pool.See docs *                                                                                                                                                                                           | string  |               |
| MONGO_MAX_IDLE_TIMEOUT_MS                                                      | Max timeout after which an idle connection is killed in milliseconds. See docs *                                                                                                                                                                                   | string  |               |
| MONGO_CONNECTION_TIMEOUT_MS                                                    | Max time to wait for a connection to be opened. See docs *                                                                                                                                                                                                         | string  |               |
| MONGO_SOCKET_TIMEOUT_MS                                                        | Max time to wait for a command send or receive before timing out. See docs *                                                                                                                                                                                       | string  |               |
| MONGO_SERVER_SELECTION_TIMEOUT_MS                                              | Max time to wait for a server to be selected while performing a communication with Mongo in milliseconds. See docs *                                                                                                                                               | string  |               |
| MONGO_WAITING_QUEUE_MS                                                         | Max time a thread has to wait for a connection to be available in milliseconds. See docs *                                                                                                                                                                         | string  |               |
| MONGO_HEARTBEAT_FREQUENCY_MS                                                   | Hearth beat frequency in milliseconds. This is an hello command that is sent periodically on each active connection to perform an health check. See docs *                                                                                                         | string  |               |
| REDIS_HOST                                                                     | Host where the redis instance used to persist idempotency keys can be found                                                                                                                                                                                        | string  |               |
| REDIS_PASSWORD                                                                 | Password used for connecting to Redis instance                                                                                                                                                                                                                     | string  |               |
| PAYMENT_TRANSACTION_GATEWAY_URI                                                | Payment transactions gateway service connection URI                                                                                                                                                                                                                | string  |               |
| PAYMENT_TRANSACTION_GATEWAY_READ_TIMEOUT                                       | Timeout for requests towards Payment transactions gateway service                                                                                                                                                                                                  | number  |               |
| PAYMENT_TRANSACTION_GATEWAY_CONNECTION_TIMEOUT                                 | Timeout for establishing connections towards Payment transactions gateway service                                                                                                                                                                                  | number  |               |
| NODO_URI                                                                       | Nodo connection URI                                                                                                                                                                                                                                                | string  |               |
| NODO_READ_TIMEOUT                                                              | Timeout for requests towards Nodo                                                                                                                                                                                                                                  | number  |               |
| NODO_CONNECTION_TIMEOUT                                                        | Timeout for establishing connections towards Nodo                                                                                                                                                                                                                  | number  |               |
| NODO_ECOMMERCE_CLIENT_ID                                                       | ecommerce clientId used for closepayment                                                                                                                                                                                                                           | string  |               |
| QUEUE_TRANSIENT_CONNECTION_STRING                                              | eCommerce storage transient connection string                                                                                                                                                                                                                      | string  |               |
| ECOMMERCE_STORAGE_TRANSIENT_QUEUE_KEY                                          | eCommerce storage transient account access key                                                                                                                                                                                                                     | string  |               |
| ECOMMERCE_STORAGE_TRANSIENT_QUEUE_ACCOUNT_NAME                                 | eCommerce storage transient account name                                                                                                                                                                                                                           | string  |               |
| ECOMMERCE_STORAGE_TRANSIENT_QUEUE_ENDPOINT                                     | eCommerce storage transient account queue endpoint                                                                                                                                                                                                                 | string  |               |
| QUEUE_DEADLETTER_CONNECTION_STRING                                             | eCommerce storage deadletter connection string                                                                                                                                                                                                                     | string  |               |˙
| TRANSACTIONS_CLOSE_PAYMENT_RETRY_QUEUE_NAME                                    | Queue name for closure events scheduled for retries                                                                                                                                                                                                                | string  |               |
| TRANSACTIONS_CLOSE_PAYMENT_QUEUE_NAME                                          | Queue name for closure events scheduled                                                                                                                                                                                                                            | string  |               |
| TRANSACTIONS_NOTIFICATIONS_RETRY_QUEUE_NAME                                    | Queue name for notification events scheduled for retries                                                                                                                                                                                                           | string  |               |
| TRANSACTIONS_NOTIFICATIONS_QUEUE_NAME                                          | Queue name for notifications events scheduler                                                                                                                                                                                                                      | string  |               |
| TRANSACTIONS_EXPIRATION_QUEUE_NAME                                             | Queue name for all events scheduled for expiration                                                                                                                                                                                                                 | string  |               |
| TRANSACTIONS_REFUND_QUEUE_NAME                                                 | Queue name for refund scheduled                                                                                                                                                                                                                                    | string  |               |
| TRANSACTIONS_REFUND_RETRY_QUEUE_NAME                                           | Queue name for refund scheduler for retries                                                                                                                                                                                                                        | string  |               |
| TRANSACTIONS_DEAD_LETTER_QUEUE_NAME                                            | Queue name were event that cannot be processed successfully are forwarded                                                                                                                                                                                          | string  |               |
| TRANSACTIONS_AUTHORIZATION_REQUESTED_QUEUE_NAME                                | Queue name for payment gateway authorization requested transactions                                                                                                                                                                                                | string  |               |
| TRANSACTIONS_AUTHORIZATION_OUTCOME_WAITING_QUEUE_NAME                          | Queue name for payment gateway authorization requested retry transactions                                                                                                                                                                                          | string  |               |
| TRANSIENT_QUEUES_TTL_SECONDS                                                   | TTL to be used when sending events on transient queues                                                                                                                                                                                                             | number  | 7 days        |
| DEAD_LETTER_QUEUE_TTL_SECONDS                                                  | TTL to be used when sending events on dead letter queues                                                                                                                                                                                                           | number  | -1 (infinite) |
| QUEUE_CONNECTION_STRING                                                        | Queue connection string used by event producers                                                                                                                                                                                                                    | string  |               |
| NOTIFICATIONS_SERVICE_URI                                                      | Notification service URI                                                                                                                                                                                                                                           | string  |               |
| NOTIFICATIONS_SERVICE_READ_TIMEOUT                                             | Notification service HTTP read timeout                                                                                                                                                                                                                             | integer |               |
| NOTIFICATIONS_SERVICE_CONNECTION_TIMEOUT                                       | Notification service HTTP connection timeout                                                                                                                                                                                                                       | integer |               |
| NOTIFICATIONS_SERVICE_API_KEY                                                  | Notification service API key                                                                                                                                                                                                                                       | string  |               |
| TRANSACTIONS_SERVICE_URI                                                       | Transaction service URI                                                                                                                                                                                                                                            | string  |               |
| TRANSACTIONS_SERVICE_READ_TIMEOUT                                              | Transaction service HTTP read timeout                                                                                                                                                                                                                              | integer |               |
| TRANSACTIONS_SERVICE_CONNECTION_TIMEOUT                                        | Transaction service HTTP connection timeout                                                                                                                                                                                                                        | integer |               |
| TRANSACTIONS_SERVICE_API_KEY                                                   | Transaction service API key                                                                                                                                                                                                                                        | string  |               |
| ECOMMERCE_PERSONAL_DATA_VAULT_API_KEY                                          | eCommerce Personal data vault API key                                                                                                                                                                                                                              | string  |               |
| WALLET_SESSION_PERSONAL_DATA_VAULT_API_KEY                                     | Wallet session Personal data vault API key                                                                                                                                                                                                                         | string  |               |
| PERSONAL_DATA_VAULT_API_BASE_PATH                                              | Persona data vault API base path                                                                                                                                                                                                                                   | string  |               |                                                                                                                                                                                                                 | integer |               |
| REFUND_RETRY_EVENT_BASE_INTERVAL_SECONDS                                       | Base interval used to calculate visibility for next retries refund event                                                                                                                                                                                           | integer |               |
| REFUND_RETRY_EVENT_MAX_ATTEMPTS                                                | Max attempts to be performed for refund                                                                                                                                                                                                                            | integer |               |
| CLOSE_PAYMENT_RETRY_EVENT_BASE_INTERVAL_SECONDS                                | Base interval used to calculate visibility for next retried closure event                                                                                                                                                                                          | integer |               |
| CLOSE_PAYMENT_RETRY_EVENT_MAX_ATTEMPTS                                         | Max attempts to be performed for close payment                                                                                                                                                                                                                     | integer |               |
| CLOSE_PAYMENT_RETRY_EVENT_PAYMENT_TOKEN_VALIDITY_TIME_OFFSET                   | Configurable offset (in seconds) that will be taken in account for payment token validity time vs retry event visibility timeout check                                                                                                                             | integer | 10 sec        |
| NOTIFICATION_RETRY_EVENT_BASE_INTERVAL_SECONDS                                 | Base interval used to calculate visibility for next retried notification event                                                                                                                                                                                     | integer |               |
| NOTIFICATION_RETRY_EVENT_MAX_ATTEMPTS                                          | Max attempts to be performed for notification                                                                                                                                                                                                                      | integer |               |
| SEND_PAYMENT_RESULT_TIMEOUT_SECONDS                                            | Max time (in seconds) to be awaited for the `sendPaymentResult` callback (`POST /user-receipts` on `transactions-service`) to be received for a given transaction                                                                                                  | integer |               |
| SEND_PAYMENT_RESULT_EXPIRATION_OFFSET_SECONDS                                  | Offset to SEND_PAYMENT_RESULT_TIMEOUT_SECONDS. Transactions that are stuck in CLOSED status for which an OK response has been received by Nodo after (SEND_PAYMENT_RESULT_TIMEOUT_SECONDS - SEND_PAYMENT_RESULT_EXPIRATION_OFFSET_SECONDS) are considered expired. | integer |               |
| NPG_URI                                                                        | NPG service URI                                                                                                                                                                                                                                                    | string  |               |
| NPG_READ_TIMEOUT                                                               | NPG service HTTP read timeout                                                                                                                                                                                                                                      | integer |               |
| NPG_CONNECTION_TIMEOUT                                                         | NPG service HTTP connection timeout                                                                                                                                                                                                                                | integer |               |
| NPG_API_KEY                                                                    | NPG service api-key                                                                                                                                                                                                                                                | string  |               |
| NPG_CARDS_PSP_KEYS                                                             | Secret structure that holds psp - api keys association for authorization request                                                                                                                                                                                   | string  |               |
| NPG_CARDS_PSP_LIST                                                             | List of all psp ids that are expected to be found into the NPG_CARDS_PSP_KEYS configuration (used for configuration cross validation)                                                                                                                              | string  |               |
| REDIS_STREAM_EVENT_CONTROLLER_STREAM_KEY                                       | Redis stream streaming key where event receiver controller commands will be write/read                                                                                                                                                                             | string  |               |
| REDIS_STREAM_EVENT_CONTROLLER_CONSUMER_GROUP_PREFIX                            | Redis event receiver controller commands stream consumer group                                                                                                                                                                                                     | string  |               |
| REDIS_STREAM_EVENT_CONTROLLER_CONSUMER_NAME_PREFIX                             | Redis event receiver controller commands stream consumer name                                                                                                                                                                                                      | string  |               |
| EVENT_CONTROLLER_STATUS_POLLING_CHRON                                          | Chron used to scheduler event receivers status polling                                                                                                                                                                                                             | string  |               |
| AUTHORIZATION_OUTCOME_WAITING_EVENT_BASE_INTERVAL_SECONDS                      | Base interval used to calculate visibility for next payment gateway auth state request event                                                                                                                                                                       | integer |               |
| AUTHORIZATION_OUTCOME_WAITING_EVENT_MAX_ATTEMPTS                               | Max attempts to be performed for payment gateway auth state request                                                                                                                                                                                                | integer |               |
| AUTHORIZATION_OUTCOME_WAITING_EVENT_PAYMENT_TOKEN_VALIDITY_TIME_OFFSET_SECONDS | The offset time to be considered when evaluating the payment token validity time remaining                                                                                                                                                                         | integer | 10 sec        |
| NODE_FORWARDER_URL                                                             | Node forwarder backend URL                                                                                                                                                                                                                                         | string  |               |
| NODE_FORWARDER_READ_TIMEOUT                                                    | Node forwarder HTTP api call read timeout in milliseconds                                                                                                                                                                                                          | integer |               |
| NODE_FORWARDER_CONNECTION_TIMEOUT                                              | Node forwarder HTTP api call connection timeout in milliseconds                                                                                                                                                                                                    | integer |               |
| REDIRECT_PAYMENT_TYPE_CODES                                                    | List of all redirect payment type codes that are expected to be present in other redirect configurations such as REDIRECT_URL_MAPPING (used for configuration cross validation)                                                                                    | string  |               |
| NODE_FORWARDER_API_KEY                                                         | Node forwarder api key                                                                                                                                                                                                                                             | string  |               |
| REDIRECT_URL_MAPPING                                                           | Key-value string map PSP to backend URI mapping that will be used for Redirect payments                                                                                                                                                                            | string  |               |
| NPG_PAYPAL_PSP_KEYS                                                            | Secret structure that holds psp - api keys association for authorization request used for APM PAYPAL payment method                                                                                                                                                | string  |               |
| NPG_PAYPAL_PSP_LIST                                                            | List of all psp ids that are expected to be found into the NPG_PAYPAL_PSP_KEYS configuration (used for configuration cross validation)                                                                                                                             | string  |               |
| NPG_BANCOMATPAY_PSP_KEYS                                                       | Secret structure that holds psp - api keys association for authorization request used for APM Bancomat pay payment method                                                                                                                                          | string  |               |
| NPG_BANCOMATPAY_PSP_LIST                                                       | List of all psp ids that are expected to be found into the NPG_BANCOMATPAY_PSP_KEYS configuration (used for configuration cross validation)                                                                                                                        | string  |               |
| NPG_MYBANK_PSP_KEYS                                                            | Secret structure that holds psp - api keys association for authorization request used for APM My bank payment method                                                                                                                                               | string  |               |
| NPG_MYBANK_PSP_LIST                                                            | List of all psp ids that are expected to be found into the NPG_MYBANK_PSP_KEYS configuration (used for configuration cross validation)                                                                                                                             | string  |               |
| NPG_SATISPAY_PSP_KEYS                                                          | Secret structure that holds psp - api keys association for authorization request used for APM Satispay payment method                                                                                                                                              | string  |               |
| NPG_SATISPAY_PSP_LIST                                                          | List of all psp ids that are expected to be found into the NPG_SATISPAY_PSP_KEYS configuration (used for configuration cross validation)                                                                                                                           | string  |               |
| NPG_APPLEPAY_PSP_KEYS                                                          | Secret structure that holds psp - api keys association for authorization request used for APM Apple pay payment method                                                                                                                                             | string  |               |
| NPG_APPLEPAY_PSP_LIST                                                          | List of all psp ids that are expected to be found into the NPG_APPLEPAY_PSP_KEYS configuration (used for configuration cross validation)                                                                                                                           | string  |               |
| DEPLOYMENT_VERSION                                                             | Env property used to identify deployment version (STAGING/PROD)                                                                                                                                                                                                    | string  | PROD          |
| NPG_REFUND_DELAY_FROM_AUTH_REQUEST_MINUTES                                     | Time to be waited, in minutes, before initializing refund process for NPG transaction starting from the authorization request date                                                                                                                                 | integer |               |
| USER_STATS_URL                                                                 | UserStats backend URL                                                                                                                                                                                                                                              | string  |               |
| USER_STATS_READ_TIMEOUT                                                        | UserStats HTTP api call read timeout in milliseconds                                                                                                                                                                                                               | integer |               |
| USER_STATS_CONNECTION_TIMEOUT                                                  | UserStats HTTP api call connection timeout in milliseconds                                                                                                                                                                                                         | integer |               |
| USER_STATS_API_KEY                                                             | UserStats api key                                                                                                                                                                                                                                                  | string  |               |
| USER_STATS_SERVICE_ENABLE_SAVE_LAST_USAGE                                      | Feature flag used to enable `save last usage` functionality                                                                                                                                                                                                        | boolean | true          |
| NPG_REFUND_EVENT_PROCESSING_DELAY_SECONDS                                 | Time to be waited, in seconds, before trying to process a refund operation from the queue                                                                                                                                                                               | integer |               |

An example configuration of these environment variables is in the `.env.example` file.

(*): for Mongo connection string options
see [docs](https://www.mongodb.com/docs/drivers/java/sync/v4.3/fundamentals/connection/connection-options/#connection-options)

## Run the application with `Docker`

Create your environment typing :

```sh
cp .env.example .env
```

Then from current project directory run :

```sh
docker-compose up
```

## Run the application with `springboot-plugin`

Create your environment:

```sh
export $(grep -v '^#' .env.local | xargs)
```

Then from current project directory run :

```sh
mvn spring-boot:run
```

Note that with this method you would also need an active Redis instance on your local machine.
We suggest you to use the [ecommerce-local](https://github.com/pagopa/pagopa-ecommerce-local) instead.

## Code formatting

Code formatting checks are automatically performed during build phase.
If the code is not well formatted an error is raised blocking the maven build.

Helpful commands:

```sh
mvn spotless:check # --> used to perform format checks
mvn spotless:apply # --> used to format all misformatted files
```

## CI

Repo has Github workflow and actions that trigger Azure devops deploy pipeline once a PR is merged on main branch.

In order to properly set version bump parameters for call Azure devops deploy pipelines will be check for the following
tags presence during PR analysis:

| Tag                | Semantic versioning scope | Meaning                                                           |
|--------------------|---------------------------|-------------------------------------------------------------------|
| patch              | Application version       | Patch-bump application version into pom.xml and Chart app version |
| minor              | Application version       | Minor-bump application version into pom.xml and Chart app version |
| major              | Application version       | Major-bump application version into pom.xml and Chart app version |
| ignore-for-release | Application version       | Ignore application version bump                                   |
| chart-patch        | Chart version             | Patch-bump Chart version                                          |
| chart-minor        | Chart version             | Minor-bump Chart version                                          |
| chart-major        | Chart version             | Major-bump Chart version                                          |
| skip-release       | Any                       | The release will be skipped altogether                            |

For the check to be successfully passed only one of the `Application version` labels and only ones of
the `Chart version` labels must be contemporary present for a given PR or the `skip-release` for skipping release step

## Dead lettering

This microservice has a built-in logic that write events to dead letter queue.
This dead letter queue is used to store all events that cannot be processed by this microservice.
There are common scenarios for which an event can be written to a dead letter queue:

1. syntactically incorrect input event
2. unhandled exception raised during event processing
3. retry attempt exhaustion for a refund retry event or refund process interruption (because of blocking error codes as
HTTP 400 for which a retry has no meaning)
4. retry attempt exhaustion for sending user notification (mail)

### Monitoring

Since an event can be written to dead letter queue for multiple reasons (see above) there is a mechanism that,
contextually to dead letter event writing to dead letter queue,
creates an OpenTelemetry span that is used to display an overall dead letter queue event dashboard.
This span has the following fields:

| field key                            | description                                                                                                                            | mandatory |
|--------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|-----------|
| deadLetterEvent.serviceName          | Constant value `pagopa-ecommerce-event-dispatcher-service`, used to uniquely identify this microservice as event writer                | ✅         |
| deadLetterEvent.category             | Enumeration of all possible errors that can make an event to be written to dead letter (see [Error category section](#Error-category)) | ✅         |
| deadLetterEvent.transactionId        | Transaction id of the input event (if the input event is parsable)                                                                     | ❌         |
| deadLetterEvent.transactionEventCode | Transaction event code of the input event (if the input event is parsable)                                                             | ❌         |

#### Error category

| Error code                            | Description                                                                                                                                  |
|---------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| SEND_PAYMENT_RESULT_RECEIVING_TIMEOUT | Transaction is stuck in CLOSED status and a sendPaymentResult has not been received before the `SEND_PAYMENT_RESULT_TIMEOUT_SECONDS` timeout |
| RETRY_EVENT_NO_ATTEMPT_LEFT           | All retry for perform a retry event have been exhausted                                                                                      |
| EVENT_PARSING_ERROR                   | Input event is not formally valid                                                                                                            |
| PROCESSING_ERROR                      | Unhandled error processing the event                                                                                                         |

### Event receiver controller

This module is essentially an asynchronous event processor.

Many processed events have a visibility timeout set to make transactions event to be process after a certain delay (such
as retry event, transaction expiration event and so on).

This makes difficult to predict when an event will be visible on the queue to be processed.

One other important aspect to take into account is the fact that module undeploy will not consider in-flight event:

module shutdown can happen when the module is in the middle of an event processing, with the possible consequence of a
broken transaction history (transaction locked in a middle event)

In order to avoid such situations an event receiver locker mechanism has been implemented using Redis Stream as fun-out
mechanism:
a group of api are directly exposed by this module and, based on request input, can initiate an event receiver orderly
shutdown.

All event receivers will then complete their processing but no new event will be taken from the queues.

After all pending event processing have been completed, the deployment can be executed with the assurance that no event
processing will be stopped in the middle of its execution.

After new version have been deployed successfully, event receiver can be start again with the same api used to stop
them.

WATCH OUT!!!! Event receivers will not be restarted automatically once module have been deployed but has to be restarted
manually.

This has been done on purpose to handle deployment rollout and avoid restarting event consumers for an old POD for which
rollout phase has not been taken into account yet.

There is a postman with api request pre-configured to start/stop consumers and to query their statuses (UP, DOWN,
UNKNOWN)

The collection is stored into this repo at -> command-postman-collection/commands.postman_collection.json
