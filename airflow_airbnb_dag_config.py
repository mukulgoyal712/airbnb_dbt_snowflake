#Configuration data for listings and reviews
config = {
    "region": "ap-south-1",
    "S3_BUCKET_NAME": "s3-snowflake-airbnb-elt",
    "listings": [
        {
            "GITHUB_REPO_listings": "https://raw.githubusercontent.com/mukulgoyal712/airbnb_dbt_snowflake/refs/heads/main/dataset/listings.csv",
            "S3_FILE_PATH_listings": "raw/listings/listings.csv"
        }
    ],
    "reviews": [
        {
            "GITHUB_REPO_reviews": "https://raw.githubusercontent.com/mukulgoyal712/airbnb_dbt_snowflake/refs/heads/main/dataset/reviews.csv",
            "S3_FILE_PATH_reviews": "raw/REVIEWS_FOLDER/reviews.csv"
        }
    ]
}

