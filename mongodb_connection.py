


# from pymongo import MongoClient
# import pandas as pd

# def connect_to_mongo(cluster_url, db_name, collection_name):
#     try:
#         # Connect to the MongoDB cluster
#         client = MongoClient(cluster_url)
        
#         # Access the database
#         db = client[db_name]

#         # Access the collection
#         collection = db[collection_name]

#         print(f"Successfully connected to database: {db_name}, collection: {collection_name}")
#         return collection
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         return None

# def get_listing_details(collection):
#     projection = {
#         'name': 1,
#         'address': 1,
#         'room_type': 1,
#         'bedrooms': 1,
#         'bathrooms': 1,
#         'amenities': 1,
#         '_id': 1  # Include the MongoDB ID field
#     }
#     return list(collection.find({}, projection))

# def get_neighborhood_details(collection):
#     projection = {
#         'address': 1,
#         'neighborhood': 1,
#         '_id': 1  # Include the MongoDB ID field
#     }
#     return list(collection.find({}, projection))

# def get_pricing_details(collection):
#     projection = {
#         'price': 1,
#         'weekly_price': 1,
#         'monthly_price': 1,
#         'security_deposit': 1,
#         'cleaning_fee': 1,
#         '_id': 1  # Include the MongoDB ID field
#     }
#     return list(collection.find({}, projection))

# def get_images(collection):
#     projection = {
#         'images.picture_url': 1,
#         '_id': 1  # Include the MongoDB ID field
#     }
#     return list(collection.find({}, projection))

# def get_host_information(collection):
#     projection = {
#         'host': 1,
#         '_id': 1  # Include the MongoDB ID field
#     }
#     return list(collection.find({}, projection))

# def get_review_details(collection):
#     projection = {
#         'reviews': 1,
#         '_id': 1  # Include the MongoDB ID field
#     }
#     return list(collection.find({}, projection))

# def get_booking_details(collection):
#     projection = {
#         'availability': 1,
#         'minimum_nights': 1,
#         'maximum_nights': 1,
#         '_id': 1  # Include the MongoDB ID field
#     }
#     return list(collection.find({}, projection))

# def convert_to_dataframe(data):
#     return pd.DataFrame(data)

# def save_to_csv(dataframe, filename):
#     dataframe.to_csv(filename, index=False)

# def main():
#     cluster_url = "mongodb+srv://julysweety21:BJ1NViQ2AZ4ItKQp@cluster0.asz94mh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
#     db_name = "sample_airbnb"
#     collection_name = "listingsAndReviews"
    
#     collection = connect_to_mongo(cluster_url, db_name, collection_name)
    
#     if collection is not None:
#         listing_details = get_listing_details(collection)
#         neighborhood_details = get_neighborhood_details(collection)
#         pricing_details = get_pricing_details(collection)
#         images = get_images(collection)
#         host_information = get_host_information(collection)
#         review_details = get_review_details(collection)
#         booking_details = get_booking_details(collection)
        
#         listing_df = convert_to_dataframe(listing_details)
#         neighborhood_df = convert_to_dataframe(neighborhood_details)
#         pricing_df = convert_to_dataframe(pricing_details)
#         images_df = convert_to_dataframe(images)
#         host_df = convert_to_dataframe(host_information)
#         review_df = convert_to_dataframe(review_details)
#         booking_df = convert_to_dataframe(booking_details)
        
#         save_to_csv(listing_df, 'listing_details.csv')
#         save_to_csv(neighborhood_df, 'neighborhood_details.csv')
#         save_to_csv(pricing_df, 'pricing_details.csv')
#         save_to_csv(images_df, 'images.csv')
#         save_to_csv(host_df, 'host_information.csv')
#         save_to_csv(review_df, 'review_details.csv')
#         save_to_csv(booking_df, 'booking_details.csv')
        
#         print("Data saved to CSV files.")
        
# if __name__ == "__main__":
#     main()

from pymongo import MongoClient
import pandas as pd

def connect_to_mongo(cluster_url, db_name, collection_name):
    try:
        # Connect to the MongoDB cluster
        client = MongoClient(cluster_url)
        
        # Access the database
        db = client[db_name]

        # Access the collection
        collection = db[collection_name]

        print(f"Successfully connected to database: {db_name}, collection: {collection_name}")
        return collection
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def get_listing_details(collection):
    projection = {
        'name': 1,
        'address': 1,
        'room_type': 1,
        'bedrooms': 1,
        'bathrooms': 1,
        'amenities': 1,
        'property_type': 1,
        'bed_type': 1,
        'number_of_reviews': 1,
        'accommodates': 1,
        'beds': 1,
        'cancellation_policy': 1,
        'review_scores': 1,
        'last_scraped': 1,
        'calendar_last_scraped': 1,
        'first_review': 1,
        'last_review': 1,
        'extra_people': 1,
        'guests_included' : 1,
        '_id': 1  # Include the MongoDB ID field
    }
    return list(collection.find({}, projection))

def get_neighborhood_details(collection):
    projection = {
        'address': 1,
        'neighbourhood': 1,           # Include neighbourhood
        'neighbourhood group': 1,  
        '_id': 1  # Include the MongoDB ID field
    }
    return list(collection.find({}, projection))

def get_pricing_details(collection):
    projection = {
        'price': 1,
        'weekly_price': 1,
        'monthly_price': 1,
        'security_deposit': 1,
        'cleaning_fee': 1,
        '_id': 1  # Include the MongoDB ID field
    }
    return list(collection.find({}, projection))

def get_images(collection):
    projection = {
        'images.picture_url': 1,
        '_id': 1  # Include the MongoDB ID field
    }
    return list(collection.find({}, projection))

def get_host_information(collection):
    projection = {
        'host': 1,
        '_id': 1  # Include the MongoDB ID field
    }
    return list(collection.find({}, projection))

def get_review_details(collection):
    projection = {
        'reviews': 1,
        '_id': 1 , # Include the MongoDB ID field
        'number_of_reviews': 1,
        'review_scores': 1,
    }
    return list(collection.find({}, projection))

def get_booking_details(collection):
    projection = {
        'availability': 1,
        'minimum_nights': 1,
        'maximum_nights': 1,
        '_id': 1  # Include the MongoDB ID field
    }
    return list(collection.find({}, projection))

def convert_to_dataframe(data):
    return pd.DataFrame(data)

def save_to_csv(dataframe, filename):
    dataframe.to_csv(filename, index=False)

def main():
    cluster_url = "mongodb+srv://julysweety21:BJ1NViQ2AZ4ItKQp@cluster0.asz94mh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    # cluster_url = "mongodb+srv://PandeeswariA:Aadhiran26@cluster0.ixp7nky.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

    db_name = "sample_airbnb"

    collection_name = "listingsAndReviews"
    
    collection = connect_to_mongo(cluster_url, db_name, collection_name)
    
    if collection is not None:
        listing_details = get_listing_details(collection)
        neighborhood_details = get_neighborhood_details(collection)
        pricing_details = get_pricing_details(collection)
        images = get_images(collection)
        host_information = get_host_information(collection)
        review_details = get_review_details(collection)
        booking_details = get_booking_details(collection)
        
        listing_df = convert_to_dataframe(listing_details)
        neighborhood_df = convert_to_dataframe(neighborhood_details)
        pricing_df = convert_to_dataframe(pricing_details)
        images_df = convert_to_dataframe(images)
        host_df = convert_to_dataframe(host_information)
        review_df = convert_to_dataframe(review_details)
        booking_df = convert_to_dataframe(booking_details)
        
        save_to_csv(listing_df, 'listing_details.csv')
        save_to_csv(neighborhood_df, 'neighborhood_details.csv')
        save_to_csv(pricing_df, 'pricing_details.csv')
        save_to_csv(images_df, 'images.csv')
        save_to_csv(host_df, 'host_information.csv')
        save_to_csv(review_df, 'review_details.csv')
        save_to_csv(booking_df, 'booking_details.csv')
        
        print("Data saved to CSV files.")
        
if __name__ == "__main__":
    main()



