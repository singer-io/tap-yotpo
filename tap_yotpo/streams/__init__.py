from .products import Products
from .product_reviews import ProductReviews
from .unsubscribers import Unsubscribers
STREAMS = {
    Products.tap_stream_id:Products,
    ProductReviews.tap_stream_id:ProductReviews,
    Unsubscribers.tap_stream_id:Unsubscribers
}