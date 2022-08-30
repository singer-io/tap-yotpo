"""tap-yotpo streams module."""
from .emails import Emails
from .orders import Orders
from .product_reviews import ProductReviews
from .products import Products
from .reviews import Reviews
from .unsubscribers import Unsubscribers
from .collections import Collections
from .customers import Customers
from .product_variants import ProductVariants
from .order_fulfillments import OrderFulfillments

STREAMS = {
    Emails.tap_stream_id: Emails,
    Orders.tap_stream_id: Orders,
    OrderFulfillments.tap_stream_id: OrderFulfillments,
    Products.tap_stream_id: Products,
    ProductReviews.tap_stream_id: ProductReviews,
    ProductVariants.tap_stream_id: ProductVariants,
    Reviews.tap_stream_id: Reviews,
    Unsubscribers.tap_stream_id: Unsubscribers,
    Collections.tap_stream_id: Collections,
    Customers.tap_stream_id: Customers
}
