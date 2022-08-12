from .emails import Emails
from .product_reviews import ProductReviews
from .products import Products
from .reviews import Reviews
from .unsubscribers import Unsubscribers

STREAMS = {
    Products.tap_stream_id:Products,
    ProductReviews.tap_stream_id:ProductReviews,
    Unsubscribers.tap_stream_id:Unsubscribers,
    Reviews.tap_stream_id:Reviews,
    Emails.tap_stream_id:Emails
}
