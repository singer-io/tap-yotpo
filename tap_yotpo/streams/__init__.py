"""tap-yotpo streams module"""
from .emails import Emails
from .product_reviews import ProductReviews
from .products import Products
from .reviews import Reviews
from .unsubscribers import Unsubscribers

STREAMS = {
    Emails.tap_stream_id: Emails,
    Products.tap_stream_id: Products,
    ProductReviews.tap_stream_id: ProductReviews,
    Reviews.tap_stream_id: Reviews,
    Unsubscribers.tap_stream_id: Unsubscribers,
}
