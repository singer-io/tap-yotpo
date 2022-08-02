from .products import Products
from .product_reviews import ProductReviews

STREAMS = {
    Products.tap_stream_id:Products,
    ProductReviews.tap_stream_id:ProductReviews
}