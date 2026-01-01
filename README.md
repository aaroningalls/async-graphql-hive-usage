# Hive Usage Extension
## For reporting your API's usage to [Hive](https://the-guild.dev/graphql/hive)

This is a runtime agnostic library, integrating seamlessly with an already functioning [async-graphql](https://docs.rs/async-graphql) API.

## General Usage
The extension buffers GraphQL requests to be sent to Hive's usage reporting. To ensure usage reporting has as little impact on the request lifecycle as possible, the actual sending of the usage report will need to take place in an async task. Creating the `HiveUsageClient` will return a task to be handled by your runtime (i.e `tokio::spawn`)

As an aside, persisted document hashes are not yet supported.

```rs
let (hive_extension, task) = HiveUsageClient::new(sender, target_id, hive_token);

let schema = GraphQLSchema::build().extension(hive_extension);
tokio::spawn(task);
```

## Sender
Along with not specifying a runtime, this extension also does not make a decision on HTTP client, allowing you to use whatever works best with your current setup. Create a struct that implements `HiveSender` to send the request to Hive, being supplied an HTTP request struct. This implementation will require `async_trait`, internally it used the version included with `async-graphql`.

## Subscriptions
Subscriptions will need to be enabled manually if you would like their usage reported to Hive. A quirk in async-graphql reports the subscription schema type as the name of your struct instead of `Subscription` if you use MergedSubscriptions.
```rs
let (mut client, _) = HiveUsageClient::new();
client = client.with_unstable_subscriptions();
```

## Builder
A few things in the extension can be modified for more advanced use cases. Use `HiveUsageClient::builder()` to gain access to these.

### `send_size`
The extension keeps a running count of approximate size of the report and sends once it hits a default of ~5MB. This value changes the default

### `should_send`
Alternatively to size checking, pass a function to more dynamically determine if/when the report should be sent. If this is supplied `send_size` and the default size calculation will be ignored. This function (if supplied) will be called on **every** GraphQL operation.

### `request_id`
Calculate a UUIDv4 to be passed as the request id for this specific report. This is primarily supplied if you would like to use the ID somewhere else in your application

### `metadata`
Hive allows each GraphQL operation to include associated metadata, a client name and version. The `ExtensionContext` is supplied to enable more dynamic values.

## HTTP clients
Some common HTTP clients are supplied with a corresponding feature flag. Generally, each client has a default or a `fn new(client)` for more flexibility and includes only default crate features.

- **reqwest** (also adds a `header_map() -> reqwest::header::HeaderMap` method to `HiveHTTPRequest`)
- **surf**
- **ureq**

## Contribution
Contribution is welcome, especially for any HTTP clients you may find useful. 