# Changelog
## 1.3.1
  * Request Timeout Implementation
## 1.3.0
  * Makes all fields `available` except primary keys and bookmarking keys [#16](https://github.com/singer-io/tap-yotpo/pull/16)
  * Updates error handling of HTTP request [#13](https://github.com/singer-io/tap-yotpo/pull/13)
  * Updates `singer-python` and `pendulum` versions [#14](https://github.com/singer-io/tap-yotpo/pull/14)
  * Fixes transform error on `unsubscribers` stream [#15](https://github.com/singer-io/tap-yotpo/pull/15)

## 1.2.0
  * Add the `domain_key` and `name` to the `product_reviews` schema [#9](https://github.com/singer-io/tap-yotpo/pull/9)

## 1.1.1
  * Add explicit dependency for pendulum [#5](https://github.com/singer-io/tap-yotpo/pull/5)

## 1.1.0
  * Update stream selection to use metadata rather than deprecated annotated-schema [#4](https://github.com/singer-io/tap-yotpo/pull/4)

## 1.0.1
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074 

## 1.0.0
  * Version bump for initial release

## 0.2.0
  * Include deleted reviews and sync backwards 30 days to pick up updated / deleted records [#1](https://github.com/singer-io/tap-yotpo/pull/1)
