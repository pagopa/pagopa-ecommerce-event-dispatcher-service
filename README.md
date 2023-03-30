# pagopa-ecommerce-event-dispatcher-service

## What is this?

This is a PagoPA microservice that handles scheduled retry mechanism for the eCommerce product.

### Environment variables

These are all environment variables needed by the application:

| Variable name                                  | Description                                                                       | type   | default |
|------------------------------------------------|-----------------------------------------------------------------------------------|--------|---------|
| MONGO_HOST                                     | Host where MongoDB instance used to persise events and view resides               | string |
| MONGO_USERNAME                                 | Username used for connecting to MongoDB instance                                  | string |         |
| MONGO_PASSWORD                                 | Password used for connecting to MongoDB instance                                  | string |         |
| MONGO_PORT                                     | Port used for connecting to MongoDB instance                                      | number |         |
| PAYMENT_TRANSACTION_GATEWAY_URI                | Payment transactions gateway service connection URI                               | string |         |
| PAYMENT_TRANSACTION_GATEWAY_READ_TIMEOUT       | Timeout for requests towards Payment transactions gateway service                 | number |         |
| PAYMENT_TRANSACTION_GATEWAY_CONNECTION_TIMEOUT | Timeout for establishing connections towards Payment transactions gateway service | number |         |
| NODO_URI                                       | Nodo connection URI                                                               | string |         |
| NODO_READ_TIMEOUT                              | Timeout for requests towards Nodo                                                 | number |         |
| NODO_CONNECTION_TIMEOUT                        | Timeout for establishing connections towards Nodo                                 | number |         |
| ECOMMERCE_STORAGE_QUEUE_KEY                    | eCommerce storage account access key                                              | string |         |
| ECOMMERCE_STORAGE_QUEUE_ACCOUNT_NAME           | eCommerce storage account name                                                    | string |         |
| ECOMMERCE_STORAGE_QUEUE_ENDPOINT               | eCommerce storage account queue endpoint                                          | string |         |
| TRANSACTION_ACTIVATED_EVENT_QUEUE_NAME         | Queue name for activated events scheduled for retries                             | string |         |
| TRANSACTIONS_CLOSE_PAYMENT_RETRY_QUEUE_NAME    | Queue name for closure events scheduled for retries                               | string |         |
| TRANSACTIONS_CLOSE_PAYMENT_QUEUE_NAME          | Queue name for closure events scheduled                                           | string |         |

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
