# Changelog

## 2.0.5
  * Bump dependency versions for twistlock compliance [#61](https://github.com/singer-io/tap-yotpo/pull/61)

## 2.0.4
  * Retry ConnectionResetErrors [#59](https://github.com/singer-io/tap-yotpo/pull/59)

## 2.0.3
  * Dependabot update [#58](https://github.com/singer-io/tap-yotpo/pull/58)

## 2.0.2
  * Dependabot update [#57](https://github.com/singer-io/tap-yotpo/pull/57)

## 2.0.1
  * Fix reviews stream to also sync deleted reviews  [#55](https://github.com/singer-io/tap-yotpo/pull/55)

## 2.0.0
  * Code refactoring [#46](https://github.com/singer-io/tap-yotpo/pull/46)
    * Add new streams `orders`, `order_fulfillments`, `product_variants` and `collections`
    * Enhance performance and bookmarking strategy for `product_reviews` stream
    * Api Version upgrade for existing streams
    * Schema changes for existing streams
    * Fix vulnerable dependency of `requests` module
    * Handle custom exceptions by providing backoff support  
    * Update unit test cases
    * Add pre-commit integration

  * Fixes following community issues :
    * Stitch Integration error https://github.com/singer-io/tap-yotpo/issues/2
    * The `emails` stream provides all historical data on each extraction https://github.com/singer-io/tap-yotpo/issues/3
    * `reviews` table field selection not applying in Stitch https://github.com/singer-io/tap-yotpo/issues/25

  * Added integration tests [#50](https://github.com/singer-io/tap-yotpo/pull/50) [#52](https://github.com/singer-io/tap-yotpo/pull/52)
  * Support for python version 3.6+

## 1.3.6
  * Unit test cases  [#38](https://github.com/singer-io/tap-yotpo/pull/38)
  * Add response of unknown errors  [#37](https://github.com/singer-io/tap-yotpo/pull/37)
## 1.3.5
  * Update products schema to accept null values [#33](https://github.com/singer-io/tap-yotpo/pull/33)
## 1.3.4
  * Exception Handling for Error 500 [#31](https://github.com/singer-io/tap-yotpo/pull/31)

## 1.3.3
  * Add null in sku field type in reviews stream to fix transformation issue [#29](https://github.com/singer-io/tap-yotpo/pull/29)

## 1.3.2
  * Storefront endpoint URL change [#27](https://github.com/singer-io/tap-yotpo/pull/27)
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
