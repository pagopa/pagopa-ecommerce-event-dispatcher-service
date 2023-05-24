# pagopa-ecommerce-event-dispatcher-service

## What is this?

This is a PagoPA microservice that handles scheduled retry mechanism for the eCommerce product.

### Environment variables

These are all environment variables needed by the application:

| Variable name                                                | Description                                                                                                                            | type    | default |
|--------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|---------|---------|
| MONGO_HOST                                                   | Host where MongoDB instance used to persise events and view resides                                                                    | string  |         |
| MONGO_USERNAME                                               | Username used for connecting to MongoDB instance                                                                                       | string  |         |
| MONGO_PASSWORD                                               | Password used for connecting to MongoDB instance                                                                                       | string  |         |
| MONGO_PORT                                                   | Port used for connecting to MongoDB instance                                                                                           | number  |         |
| MONGO_SSL_ENABLED                                            | Boolean value indicating if use SSL for connecting to MongoDB instance                                                                 | boolean |         |
| PAYMENT_TRANSACTION_GATEWAY_URI                              | Payment transactions gateway service connection URI                                                                                    | string  |         |
| PAYMENT_TRANSACTION_GATEWAY_READ_TIMEOUT                     | Timeout for requests towards Payment transactions gateway service                                                                      | number  |         |
| PAYMENT_TRANSACTION_GATEWAY_CONNECTION_TIMEOUT               | Timeout for establishing connections towards Payment transactions gateway service                                                      | number  |         |
| NODO_URI                                                     | Nodo connection URI                                                                                                                    | string  |         |
| NODO_READ_TIMEOUT                                            | Timeout for requests towards Nodo                                                                                                      | number  |         |
| NODO_CONNECTION_TIMEOUT                                      | Timeout for establishing connections towards Nodo                                                                                      | number  |         |
| ECOMMERCE_STORAGE_QUEUE_KEY                                  | eCommerce storage account access key                                                                                                   | string  |         |
| ECOMMERCE_STORAGE_QUEUE_ACCOUNT_NAME                         | eCommerce storage account name                                                                                                         | string  |         |
| ECOMMERCE_STORAGE_QUEUE_ENDPOINT                             | eCommerce storage account queue endpoint                                                                                               | string  |         |
| TRANSACTIONS_CLOSE_PAYMENT_RETRY_QUEUE_NAME                  | Queue name for closure events scheduled for retries                                                                                    | string  |         |
| TRANSACTIONS_CLOSE_PAYMENT_QUEUE_NAME                        | Queue name for closure events scheduled                                                                                                | string  |         |
| TRANSACTIONS_NOTIFICATIONS_RETRY_QUEUE_NAME                  | Queue name for notification events scheduled for retries                                                                               | string  |         |
| TRANSACTIONS_NOTIFICATIONS_QUEUE_NAME                        | Queue name for notifications events scheduler                                                                                          | string  |         |
| TRANSACTIONS_EXPIRATION_QUEUE_NAME                           | Queue name for all events scheduled for expiration                                                                                     | string  |         |
| TRANSACTIONS_REFUND_QUEUE_NAME                               | Queue name for refund scheduled                                                                                                        | string  |         |
| TRANSACTIONS_REFUND_RETRY_QUEUE_NAME                         | Queue name for refund scheduler for retries                                                                                            | string  |         |
| QUEUE_CONNECTION_STRING                                      | Queue connection string used by event producers                                                                                        | string  |         |
| NOTIFICATIONS_SERVICE_URI                                    | Notification service URI                                                                                                               | string  |         |
| NOTIFICATIONS_SERVICE_READ_TIMEOUT                           | Notification service HTTP read timeout                                                                                                 | integer |         |
| NOTIFICATIONS_SERVICE_CONNECTION_TIMEOUT                     | Notification service HTTP connection timeout                                                                                           | integer |         |
| NOTIFICATIONS_SERVICE_API_KEY                                | Notification service API key                                                                                                           | string  |         |
| PERSONAL_DATA_VAULT_API_KEY                                  | Personal data vault API key                                                                                                            | string  |         |
| PERSONAL_DATA_VAULT_API_BASE_PATH                            | Persona data vault API base path                                                                                                       | string  |         |
| PAYMENT_TRANSACTIONS_GATEWAY_URI                             | Payment transaction gateway URI                                                                                                        | string  |         |
| PAYMENT_TRANSACTIONS_GATEWAY_READ_TIMEOUT                    | Payment transaction gateway HTTP read timeout                                                                                          | integer |         |
| PAYMENT_TRANSACTIONS_GATEWAY_CONNECTION_TIMEOUT              | Payment transaction gateway HTTP connection timeout                                                                                    | integer |         |
| PAYMENT_TRANSACTIONS_GATEWAY_API_KEY                         | Payment transaction gateway API subscription-key                                                                                       | integer |         |
| REFUND_RETRY_EVENT_BASE_INTERVAL_SECONDS                     | Base interval used to calculate visibility for next retries refund event                                                               | integer |         |
| REFUND_RETRY_EVENT_MAX_ATTEMPTS                              | Max attempts to be performed for refund                                                                                                | integer |         |
| CLOSE_PAYMENT_RETRY_EVENT_BASE_INTERVAL_SECONDS              | Base interval used to calculate visibility for next retried closure event                                                              | integer |         |
| CLOSE_PAYMENT_RETRY_EVENT_MAX_ATTEMPTS                       | Max attempts to be performed for close payment                                                                                         | integer |         |
| CLOSE_PAYMENT_RETRY_EVENT_PAYMENT_TOKEN_VALIDITY_TIME_OFFSET | Configurable offset (in seconds) that will be taken in account for payment token validity time vs retry event visibility timeout check | integer | 10 sec  |
| NOTIFICATION_RETRY_EVENT_BASE_INTERVAL_SECONDS               | Base interval used to calculate visibility for next retried notification event                                                         | integer |         |
| NOTIFICATION_RETRY_EVENT_MAX_ATTEMPTS                        | Max attempts to be performed for notification                                                                                          | integer |         |

An example configuration of these environment variables is in the `.env.example` file.

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
