from .products import Products
from .product_reviews import ProductReviews
from .unsubscribers import Unsubscribers
from .reviews import Reviews

STREAMS = {
    Products.tap_stream_id:Products,
    ProductReviews.tap_stream_id:ProductReviews,
    Unsubscribers.tap_stream_id:Unsubscribers,
    Reviews.tap_stream_id:Reviews
}