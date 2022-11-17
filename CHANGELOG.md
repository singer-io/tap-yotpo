# Changelog

## 2.0.0
  * Code Refactoring [#46](https://github.com/singer-io/tap-yotpo/pull/46)
    - New streams added from V3 Yotpo Api: `orders`, `order_fulfillments`, `product_variants` and `collections`
    - Performance enhancement, support for interruptible sync & bookmarking fix for `product_reviews` stream
    - Api Version Upgrade for existing streams
    - Improved folder structure
    - Schema Changes for existing streams
    - Fixed vulnerable dependency of `requests` module
    - Updated unit test cases
    - Added pre-commit integration
    - Added standard linter & formatter config with pyproject.toml' & setup.cfg` files
    - Code refactoring, type hinting & increased readability

  * Added Integration Tests [#50](https://github.com/singer-io/tap-yotpo/pull/50)
    - Formatting changes [#52](https://github.com/singer-io/tap-yotpo/pull/52)

  * Fixes Following issues :
    - https://github.com/singer-io/tap-yotpo/issues/2
    - https://github.com/singer-io/tap-yotpo/issues/3
    - https://github.com/singer-io/tap-yotpo/issues/25

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
