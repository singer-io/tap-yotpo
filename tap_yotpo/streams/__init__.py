"""tap-yotpo streams module."""
from .collections import Collections
from .emails import Emails
from .order_fulfillments import OrderFulfillments
from .orders import Orders
from .product_reviews import ProductReviews
from .product_variants import ProductVariants
from .products import Products
from .reviews import Reviews
from .unsubscribers import Unsubscribers

STREAMS = {
    Collections.tap_stream_id: Collections,
    Emails.tap_stream_id: Emails,
    OrderFulfillments.tap_stream_id: OrderFulfillments,
    Orders.tap_stream_id: Orders,
    ProductReviews.tap_stream_id: ProductReviews,
    ProductVariants.tap_stream_id: ProductVariants,
    Products.tap_stream_id: Products,
    Reviews.tap_stream_id: Reviews,
    Unsubscribers.tap_stream_id: Unsubscribers,
}
